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
package org.kitesdk.morphline.hadoop.parquet.avro;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Metrics;
import org.kitesdk.morphline.stdio.AbstractParser;

import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;

import com.codahale.metrics.Meter;
import org.kitesdk.morphline.shaded.com.google.common.io.Closeables;
import com.typesafe.config.Config;


/**
 * Command that parses a Hadoop File that contains Parquet data; for each Parquet Avro
 * datum, the command emits a morphline record containing the record as an attachment in
 * {@link Fields#ATTACHMENT_BODY}.
 * 
 * The Avro schema that was used to write the data is retrieved from the Parquet file. Optionally, the
 * Avro schema that shall be used for reading can be supplied as well.
 */
public final class ReadAvroParquetFileBuilder implements CommandBuilder {

  /** The morphline record field containing the HDFS Path of the Parquet file to read */
  public static final String FILE_UPLOAD_URL = "file_upload_url"; // copied from HdfsFileFieldNames

  /** The MIME type identifier that will be filled into output records */
  public static final String AVRO_MEMORY_MIME_TYPE = "avro/java+memory";

  
  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("readAvroParquetFile");
  }
  
  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ReadAvroParquetFile(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class ReadAvroParquetFile extends AbstractCommand {

    private final Configuration conf;
    private final Meter numRecordsMeter;
    
    public ReadAvroParquetFile(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {   
      super(builder, config, parent, child, context);

      this.conf = new Configuration();
      String defaultFileSystemUri = getConfigs().getString(config, "fs", null);
      if (defaultFileSystemUri != null) {
        FileSystem.setDefaultUri(conf, defaultFileSystemUri); // see Hadoop's GenericOptionsParser
      }
      for (String value : getConfigs().getStringList(config, "conf", Collections.<String>emptyList())) {
        conf.addResource(new Path(value)); // see Hadoop's GenericOptionsParser
      }
      
      // configure projection schema, if any
      String projectionSchemaString = getConfigs().getString(config, "projectionSchemaString", null);
      Schema projectionSchema;
      if (projectionSchemaString != null) {
        projectionSchema = new Parser().parse(projectionSchemaString);
      } else {        
        String projectionSchemaFile = getConfigs().getString(config, "projectionSchemaFile", null);
        if (projectionSchemaFile != null) {
          try { 
            projectionSchema = new Parser().parse(new File(projectionSchemaFile));
          } catch (IOException e) {
            throw new MorphlineCompilationException("Cannot parse external Avro projection schema file: " + projectionSchemaFile, config, e);
          }
        } else {
          projectionSchema = null;
        }
      }      
      
      if (projectionSchema != null) {
        AvroReadSupport.setRequestedProjection(conf, projectionSchema);
      }
      
      // configure reader schema, if any
      String readerSchemaString = getConfigs().getString(config, "readerSchemaString", null);
      Schema readerSchema;
      if (readerSchemaString != null) {
        readerSchema = new Parser().parse(readerSchemaString);
      } else {        
        String readerSchemaFile = getConfigs().getString(config, "readerSchemaFile", null);
        if (readerSchemaFile != null) {
          try { 
            readerSchema = new Parser().parse(new File(readerSchemaFile));
          } catch (IOException e) {
            throw new MorphlineCompilationException("Cannot parse external Avro reader schema file: " + readerSchemaFile, config, e);
          }
        } else {
          readerSchema = null;
        }
      }      
      
      if (readerSchema != null) {
        AvroReadSupport.setAvroReadSchema(conf, readerSchema);
      }
      
      this.numRecordsMeter = getMeter(Metrics.NUM_RECORDS);
      
      validateArguments();      
    }
    
    @Override
    protected boolean doProcess(Record inputRecord) {
      List paths = inputRecord.get(FILE_UPLOAD_URL);
      if (paths.size() == 0) {
        return false;
      }
      Path path;
      Object obj = paths.get(0);
      if (obj instanceof Path) {
        path = (Path) obj;
      } else {
        path = new Path(obj.toString());
      }
      
      Record template = inputRecord.copy();
      AbstractParser.removeAttachments(template);
      template.put(Fields.ATTACHMENT_MIME_TYPE, AVRO_MEMORY_MIME_TYPE);

      AvroParquetReader<IndexedRecord> reader = null;
      try {
        reader = new AvroParquetReader(conf, path);
        while (true) {
          IndexedRecord datum;
          try {
            datum = reader.read();
          } catch (EOFException e) {
            return true; // be lenient
          }

          if (datum == null) {
            return true; // EOS
          }
          if (!extract(datum, template)) {
            return false;
          }
        }
      } catch (IOException e) {
        throw new MorphlineRuntimeException(e);
      } finally {
        try {
          Closeables.closeQuietly(reader);
        } catch (NullPointerException e) {
          ; // tmp work-around for https://issues.apache.org/jira/browse/PARQUET-145
        }
      }
    }
        
    private boolean extract(GenericContainer datum, Record inputRecord) {
      incrementNumRecords();
      Record outputRecord = inputRecord.copy();
      outputRecord.put(Fields.ATTACHMENT_BODY, datum);
        
      // pass record to next command in chain:
      return getChild().process(outputRecord);
    }
    
    private void incrementNumRecords() {
      if (isMeasuringMetrics()) {
        numRecordsMeter.mark();
      }
    }

  }
  
}
