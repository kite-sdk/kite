/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.morphline;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReaderException;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.spi.ReaderWriterState;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.FaultTolerance;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Metrics;
import org.kitesdk.morphline.base.Notifications;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.common.annotations.Beta;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * A DatasetWriter that uses a morphline to transform it's input and send the resulting output to any other DatasetWriter.
 */
@Beta
public class MorphlineDatasetWriter<INPUT,OUTPUT> implements DatasetWriter<INPUT> {

  private final DatasetWriter<OUTPUT> childWriter;
  private final DatasetDescriptor descriptor;
  private ReaderWriterState state;
  
  private final MorphlineContext morphlineContext;
  private final Command morphline;
  private final String morphlineFileAndId;
  
  private final Timer mappingTimer;
  private final Meter numRecords;
  private final Meter numFailedRecords;
  private final Meter numExceptionRecords;
  
  public static final String MORPHLINE_FILE_PARAM = "morphlineFile";
  public static final String MORPHLINE_FILE_CONTENTS_PARAM = "morphlineFileContents";
  public static final String MORPHLINE_ID_PARAM = "morphlineId";
  
  /**
   * Morphline variables can be passed from DatasetDescriptor properties to the morphline, e.g.
   * morphlineVariable.zkHost=127.0.0.1:2181/solr
   */
  public static final String MORPHLINE_VARIABLE_PARAM = "morphlineVariable";
  
  private static final Logger logger = LoggerFactory
      .getLogger(MorphlineDatasetWriter.class);


  public MorphlineDatasetWriter(DatasetWriter<OUTPUT> child, DatasetDescriptor descriptor) {
    this(child, descriptor, null);
  }
  
  public MorphlineDatasetWriter(DatasetWriter<OUTPUT> childWriter, DatasetDescriptor descriptor, MorphlineContext context) {
    Preconditions.checkArgument(childWriter != null, "Child DatasetWriter cannot be null");
    Preconditions.checkArgument(descriptor != null, "DatasetDescriptor cannot be null");
    this.childWriter = childWriter;
    this.descriptor = descriptor;

    String morphlineFile = descriptor.getProperty(MORPHLINE_FILE_PARAM);
    String morphlineFileContents = descriptor.getProperty(MORPHLINE_FILE_CONTENTS_PARAM);
    File tmpMorphlineFile = null;
    try {
      if (morphlineFileContents != null) {
        try {
          tmpMorphlineFile = File.createTempFile("morphlines", ".conf");
          Files.write(morphlineFileContents, tmpMorphlineFile, Charsets.UTF_8);
          morphlineFile = tmpMorphlineFile.getPath();
        } catch (IOException e) {
          throw new DatasetReaderException(e);
        }
      }
      if (morphlineFile == null || morphlineFile.trim().length() == 0) {
        throw new IllegalArgumentException("Missing parameter: " + MORPHLINE_FILE_PARAM, null);
      }
      String morphlineId = descriptor.getProperty(MORPHLINE_ID_PARAM);
      this.morphlineFileAndId = morphlineFile + "@" + morphlineId;
      
      if (context == null) {
        FaultTolerance faultTolerance = new FaultTolerance(
            getBoolean(descriptor, FaultTolerance.IS_PRODUCTION_MODE, false), 
            getBoolean(descriptor, FaultTolerance.IS_IGNORING_RECOVERABLE_EXCEPTIONS, false),
            descriptor.getProperty(FaultTolerance.RECOVERABLE_EXCEPTION_CLASSES));
        
        context = new MorphlineContext.Builder()
          .setExceptionHandler(faultTolerance)
          .setMetricRegistry(SharedMetricRegistries.getOrCreate(morphlineFileAndId))
          .build();
      }   
      this.morphlineContext = context;
  
      Map<String, Object> morphlineVariables = Maps.newHashMap();
      for (String name : descriptor.listProperties()) {
          String variablePrefix = MORPHLINE_VARIABLE_PARAM + ".";
          if (name.startsWith(variablePrefix)) {
              morphlineVariables.put(name.substring(variablePrefix.length()), descriptor.getProperty(name));
          }
      }
      Config override = ConfigFactory.parseMap(morphlineVariables);
  
      this.morphline = new Compiler().compile(new File(morphlineFile), 
          morphlineId, morphlineContext, new Collector(), override);      
    } finally {
      if (tmpMorphlineFile != null && !tmpMorphlineFile.delete()) {
        logger.warn("Cannot delete file: {}", tmpMorphlineFile);
      }
    }
    
    this.mappingTimer = morphlineContext.getMetricRegistry().timer(
        MetricRegistry.name(Metrics.MORPHLINE_APP, Metrics.ELAPSED_TIME));
    this.numRecords = morphlineContext.getMetricRegistry().meter(
        MetricRegistry.name(Metrics.MORPHLINE_APP, Metrics.NUM_RECORDS));
    this.numFailedRecords = morphlineContext.getMetricRegistry().meter(
        MetricRegistry.name(Metrics.MORPHLINE_APP, Metrics.NUM_FAILED_RECORDS));
    this.numExceptionRecords = morphlineContext.getMetricRegistry().meter(
        MetricRegistry.name(Metrics.MORPHLINE_APP, Metrics.NUM_EXCEPTION_RECORDS));
    
    this.state = ReaderWriterState.NEW;
  }

  @Override
  public void open() {
    Preconditions.checkState(state.equals(ReaderWriterState.NEW),
      "Unable to open a writer from state:%s", state);

    childWriter.open();
    state = ReaderWriterState.OPEN;
    Notifications.notifyBeginTransaction(morphline);
  }

  @Override
  public void write(INPUT entity) {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
      "Attempt to write to a writer in state:%s", state);

    numRecords.mark();
    Timer.Context timerContext = mappingTimer.time();
    try {
      Record record = new Record();
      record.put(Fields.ATTACHMENT_BODY, entity);
      record.put("_dataset_descriptor_schema", descriptor.getSchema()); // e.g. for toAvro command
      try {
        Notifications.notifyStartSession(morphline);
        if (!morphline.process(record)) {
          numFailedRecords.mark();
          logger.warn("Morphline {} failed to process record: {}", morphlineFileAndId, record);
        }
      } catch (RuntimeException t) {
        numExceptionRecords.mark();
        morphlineContext.getExceptionHandler().handleException(t, record);
      }
    } finally {
      timerContext.stop();
    }
  }

  @Override
  public void flush() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
      "Attempt to write to a writer in state:%s", state);

    Notifications.notifyCommitTransaction(morphline);
    Notifications.notifyBeginTransaction(morphline);
    childWriter.flush();
  }

  @Override
  public void close() {
    if (state.equals(ReaderWriterState.OPEN)) {
      Notifications.notifyShutdown(morphline);
      childWriter.close();
      state = ReaderWriterState.CLOSED;
    }
  }

  @Override
  public boolean isOpen() {
    return state.equals(ReaderWriterState.OPEN);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("descriptor", descriptor)
      .add("state", state)
      .add("childWriter", childWriter)
      .toString();
  }

  private boolean getBoolean(DatasetDescriptor descriptor, String key, boolean defaultValue) {
    String value = descriptor.getProperty(key);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return defaultValue;
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private final class Collector implements Command {
    
    @Override
    public Command getParent() {
      return null;
    }
    
    @Override
    public void notify(Record notification) {
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean process(Record record) {
      Preconditions.checkNotNull(record);
      Object result = record.getFirstValue(Fields.ATTACHMENT_BODY);
      Preconditions.checkNotNull(result);
      childWriter.write((OUTPUT) result);
      return true;
    }
    
  }

}
