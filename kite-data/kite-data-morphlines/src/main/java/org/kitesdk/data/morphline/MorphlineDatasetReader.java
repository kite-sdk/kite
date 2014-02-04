/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import java.io.InputStream;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReaderException;
import org.kitesdk.data.spi.AbstractDatasetReader;
import org.kitesdk.data.spi.ReaderWriterState;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineCompilationException;
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
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * A DataSetReader that integrates morphlines. The MorphlineDataSetReader opens an HDFS file, runs a
 * (configurable) pipelined morphline over it, and converts the morphline output to Avro objects,
 * which are then returned by the DataSetReader pull iterator.
 */
@Beta
public class MorphlineDatasetReader<E> extends AbstractDatasetReader<E> {

  private final FileSystem fs;
  private final Path path;
  private final DatasetDescriptor descriptor;
  private final Schema schema;

  private ReaderWriterState state = ReaderWriterState.NEW;
  private boolean hasLookAhead = false;  
  private E next = null;  

  private final BlockingQueue<Record> queue;
  private CountDownLatch isClosing;
  private Thread producerThread;
  private final Throwable[] producerExceptionHolder = new Throwable[1];
  
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
  public static final String MORPHLINE_QUEUE_CAPACITY = "morphlineQueueCapacity";
  
  /**
   * Morphline variables can be passed from DatasetDescriptor properties to the morphline, e.g.
   * morphlineVariable.zkHost=127.0.0.1:2181/solr
   */
  public static final String MORPHLINE_VARIABLE_PARAM = "morphlineVariable";
  
  private static final Record EOS = new Record(); // sentinel

  private static final Logger logger = LoggerFactory.getLogger(MorphlineDatasetReader.class);

  
  public MorphlineDatasetReader(FileSystem fileSystem, Path path, DatasetDescriptor descriptor) {
    this(fileSystem, path, descriptor, null);
  }
  
  public MorphlineDatasetReader(FileSystem fileSystem, Path path, DatasetDescriptor descriptor, 
      MorphlineContext context) {
    
    Preconditions.checkArgument(fileSystem != null, "FileSystem cannot be null");
    Preconditions.checkArgument(path != null, "Path cannot be null");
    Preconditions.checkArgument(descriptor != null, "DatasetDescriptor cannot be null");

    //reset(fileSystem, path);
    this.fs = fileSystem;
    this.path = path;
    this.state = ReaderWriterState.NEW;
    
    this.descriptor = descriptor;
    this.schema = descriptor.getSchema();
    Preconditions.checkArgument(schema != null, "Schema cannot be null");
    Preconditions.checkArgument(Schema.Type.RECORD.equals(schema.getType()),
        "Schemas for morphline files must be records of primitive types");
    
    this.queue = new ArrayBlockingQueue<Record>(getInteger(descriptor, MORPHLINE_QUEUE_CAPACITY, 1000));
    
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
      String morphlineId = descriptor.getProperty(MORPHLINE_ID_PARAM);
      if (morphlineFile == null || morphlineFile.trim().length() == 0) {
        throw new MorphlineCompilationException("Missing parameter: " + MORPHLINE_FILE_PARAM, null);
      }
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
  }
  
//  private void reset(FileSystem fileSystem, Path path) {
//    this.fs = fileSystem;
//    this.path = path;
//    this.state = ReaderWriterState.NEW;
//  }

  @Override
  public boolean isOpen() {
    return (this.state == ReaderWriterState.OPEN);
  }

  @SuppressWarnings("unchecked")
  public void open() {
    Preconditions.checkState(state.equals(ReaderWriterState.NEW),
        "A reader may not be opened more than once - current state:%s", state);

    hasLookAhead = false;  
    next = null;
    producerExceptionHolder[0] = null;
    isClosing = new CountDownLatch(1);
    queue.clear();

    final InputStream inputStream;
    try {
      inputStream = fs.open(path);
    } catch (IOException ex) {
      throw new DatasetReaderException("Cannot open path: " + path, ex);
    }

    
    producerThread = new Thread(new Runnable() {

      @Override
      public void run() { 
        try {
          doRun();
        } catch (Throwable e) {
          synchronized (producerExceptionHolder) {
            producerExceptionHolder[0] = e; // forward exception to consumer thread
          }
          logger.warn("Morphline producer thread exception", e);
        }
      }
      
      private void doRun() {
        try {
          Timer.Context timerContext = mappingTimer.time();
          try {
            Record record = new Record();
            record.put(Fields.ATTACHMENT_BODY, inputStream);
            record.put("_dataset_descriptor_schema", schema); // e.g. for toAvro command
            try {
              Notifications.notifyBeginTransaction(morphline);
              Notifications.notifyStartSession(morphline);
              if (!morphline.process(record)) {
                numFailedRecords.mark();
                logger.warn("Morphline {} failed to process record: {}", morphlineFileAndId, record);
              }
              Notifications.notifyCommitTransaction(morphline);
            } catch (RuntimeException t) {
              numExceptionRecords.mark();
              morphlineContext.getExceptionHandler().handleException(t, record);
            } catch (CloseSignal e) {
              ; // nothing to do
            } 
          } finally {
            timerContext.stop();
            Closeables.closeQuietly(inputStream);
            //Notifications.notifyShutdown(morphline);            
          }
        } finally {
          while (isClosing.getCount() > 0) {
            try {
              // Signal that we've reached EOS
              if (queue.offer(EOS, 100, TimeUnit.MILLISECONDS)) {
                break; // success - we're done
              }
            } catch (InterruptedException e) {
              ;
            }
          }      
        }
      }
      
    });
    
    producerThread.start();
    state = ReaderWriterState.OPEN;
  }

  @Override
  public boolean hasNext() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to read from a file in state:%s", state);
    
    if (!hasLookAhead) {
      next = advance();
      hasLookAhead = true;
    }
    return next != null;
  }

  @Override
  public E next() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to read from a file in state:%s", state);

    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    hasLookAhead = false;
    return next;
  }

  @SuppressWarnings("unchecked")
  private E advance() {
    Record morphlineRecord;
    try {
      morphlineRecord = queue.take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    
    assert morphlineRecord != null;
    if (morphlineRecord == EOS) {
      Throwable t; 
      synchronized (producerExceptionHolder) {
        t = producerExceptionHolder[0];
      }
      if (t != null) {
        throw new DatasetReaderException(t);
      }
      return null;
    }
    
    numRecords.mark();
    Object body = morphlineRecord.getFirstValue(Fields.ATTACHMENT_BODY);    
    return (E) body;
  }

  @Override
  public void close() {
    if (isClosing != null) {
      isClosing.countDown();    
    }
    if (!state.equals(ReaderWriterState.OPEN)) {
      return;
    }
    logger.debug("Closing reader on path: {}", path);
    state = ReaderWriterState.CLOSED;
    try {
      if (producerThread != null) {
        producerThread.join();
      }
    } catch (InterruptedException e) {
      ;
    } finally {
      queue.clear();
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("fileSystem", fs)
      .add("path", path)
      .add("descriptor", descriptor)
      .add("state", state)
      .toString();
  }

  private boolean getBoolean(DatasetDescriptor descriptor, String key, boolean defaultValue) {
    String value = descriptor.getProperty(key);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return defaultValue;
  }

  private int getInteger(DatasetDescriptor descriptor, String key, int defaultValue) {
    String value = descriptor.getProperty(key);
    if (value != null) {
      return Integer.parseInt(value);
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
    public boolean process(Record record) {
      Preconditions.checkNotNull(record);
      while (true) {
        if (isClosing.getCount() <= 0) {
          throw new CloseSignal();
        }
        try {
          if (queue.offer(record, 100, TimeUnit.MILLISECONDS)) {
            return true;
          }
        } catch (InterruptedException e) {
          throw new CloseSignal();
        }
      }      
    }
    
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class CloseSignal extends Error {    
  }
}