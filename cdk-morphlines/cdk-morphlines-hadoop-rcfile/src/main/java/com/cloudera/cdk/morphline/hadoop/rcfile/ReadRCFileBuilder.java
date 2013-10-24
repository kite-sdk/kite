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
package com.cloudera.cdk.morphline.hadoop.rcfile;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.CommandBuilder;
import com.cloudera.cdk.morphline.api.MorphlineCompilationException;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.MorphlineRuntimeException;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.Configs;
import com.cloudera.cdk.morphline.base.Fields;
import com.cloudera.cdk.morphline.base.Validator;
import com.cloudera.cdk.morphline.stdio.AbstractParser;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.typesafe.config.Config;

/**
 * Command for reading Record Columnar (RC) Files. RCFile can be read in two
 * ways
 * 
 * 1. Row Wise - Emits one record for every row in the RCFile, with columns
 * mapped with names as provided in the column map
 * 
 * 2. Column Wise - Emits one record for every row in one column, before moving
 * to the next column (in the order provided in the column map)
 * 
 * For a RCFile with 5 rows and 5 columns. Row wise would emit 5 records and
 * Column wise would emit 25 records. De-Compression is handled automatically
 * 
 */
public final class ReadRCFileBuilder implements CommandBuilder {
  
  public static final String OUTPUT_MEDIA_TYPE = "application/java-rc-file-record";
  public static final String RC_FILE_META_DATA = "RCFileMetaData";

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("readRCFile");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ReadRCFile(this, config, parent, child, context);
  }
  

  // /////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  // /////////////////////////////////////////////////////////////////////////////
  private static final class ReadRCFile extends AbstractParser {
    
    private final boolean includeMetaData;
    private final RCFileReadMode readMode;
    private final Map<Integer, RCFileColumn> columns = Maps.newHashMap();
    private final Configuration conf = new Configuration();

    private static final Object STREAM_PROTOCOL = "stream://";

    public ReadRCFile(CommandBuilder builder, Config config, Command parent,
        Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);
      this.includeMetaData = getConfigs().getBoolean(config, "includeMetaData", false);
      this.readMode = new Validator<RCFileReadMode>()
          .validateEnum(
              config,
              getConfigs().getString(config, "readMode",
                  RCFileReadMode.row.name()), RCFileReadMode.class);
      
      parseColumnMap(config);
      validateArguments();
    }

    private void parseColumnMap(final Config config) {
      for (Config columnMapConfig : getConfigs().getConfigList(config, "columns")) {
        Configs configs = new Configs();
        int inputField = configs.getInt(columnMapConfig, "inputField");
        if (inputField < 0) {
          throw new MorphlineCompilationException(
              "Invalid column inputField specified: " + inputField, columnMapConfig);
        }

        String fieldName = configs.getString(columnMapConfig, "outputField");
        String writableClassString = configs.getString(columnMapConfig, "writableClass");

        if (writableClassString == null || writableClassString.isEmpty()) {
          throw new MorphlineCompilationException(
              "No writableClass specified for column " + fieldName, columnMapConfig);
        }
        Class<Writable> writableClass;
        try {
          Class clazz = Class.forName(writableClassString);
          if (!Writable.class.isAssignableFrom(clazz)) {
            throw new MorphlineCompilationException("writableClass provided "
                + writableClassString + " for column " + fieldName
                + " does not implement " + Writable.class.getName(), columnMapConfig);
          }
          writableClass = clazz;
        } catch (ClassNotFoundException e) {
          throw new MorphlineCompilationException("Could not load class "
              + writableClassString + " definition", columnMapConfig, e);
        }
        columns.put(inputField, new RCFileColumn(fieldName, writableClass, conf));
        configs.validateArguments(columnMapConfig);
      }
    }

    @Override
    protected boolean doProcess(Record record, InputStream in)
        throws IOException {
      Path attachmentPath = getAttachmentPath(record);
      SingleStreamFileSystem fs = new SingleStreamFileSystem(in, attachmentPath);
      RCFile.Reader reader = null;
      try {
        reader = new RCFile.Reader(fs, attachmentPath, conf);
        Record template = record.copy();
        removeAttachments(template);
        template.put(Fields.ATTACHMENT_MIME_TYPE, OUTPUT_MEDIA_TYPE);
        if (includeMetaData) {
          SequenceFile.Metadata metadata = reader.getMetadata();
          if (metadata != null) {
            template.put(RC_FILE_META_DATA, metadata);
          }
        }
        
        switch (readMode) {
        case row:
          return readRowWise(reader, template);
        case column:
          return readColumnWise(reader, template);
        }
      } catch (IOException e) {
        throw new MorphlineRuntimeException(
            "IOException while processing attachment "
                + attachmentPath.getName(), e);
      } finally {
        if (reader != null) {
          reader.close();
        }
      }
      return true;
    }

    private Path getAttachmentPath(final Record record) {
      // We have more meaningful RCFile error messages
      // if we have a attachment name
      String attachmentName = (String) record.getFirstValue(Fields.ATTACHMENT_NAME);
      if (attachmentName == null) {
        attachmentName = "UNKNOWN";
      }
      return new Path(STREAM_PROTOCOL + attachmentName);
    }

    private boolean readRowWise(final RCFile.Reader reader, final Record record)
        throws IOException {
      
      LongWritable rowID = new LongWritable();      
      BytesRefArrayWritable rowBatchBytes = new BytesRefArrayWritable();

      while (true) {
        boolean next;
        try {
          next = reader.next(rowID);
        } catch (EOFException ex) {
          // We have hit EOF of the stream
          break;
        }

        if (!next) {
          break;
        }

        incrementNumRecords();
        Record outputRecord = record.copy();        
        rowBatchBytes.resetValid(columns.size());
        reader.getCurrentRow(rowBatchBytes);

        // Read all the columns configured and set it in the output record
        for (Map.Entry<Integer, RCFileColumn> columnMapEntry : columns.entrySet()) {
          RCFileColumn rcColumn = columnMapEntry.getValue();
          Integer columnIndex = columnMapEntry.getKey();
          BytesRefWritable columnBytes = rowBatchBytes.get(columnIndex);
          outputRecord.put(rcColumn.getOutputField(), updateColumnValue(rcColumn, columnBytes));
        }
        
        // pass record to next command in chain:
        if (!getChild().process(outputRecord)) {
          return false;
        }
      }
      return true;
    }

    private boolean readColumnWise(RCFile.Reader reader, Record record) throws IOException {
      
      for (Map.Entry<Integer, RCFileColumn> columnMapEntry : columns.entrySet()) {
        RCFileColumn rcColumn = columnMapEntry.getValue();
        Integer columnIndex = columnMapEntry.getKey();
        reader.sync(0);
        reader.resetBuffer();
        while (true) {
          boolean next;
          try {
            next = reader.nextBlock();
          } catch (EOFException ex) {
            // We have hit EOF of the stream
            break;
          }

          if (!next) {
            break;
          }

          BytesRefArrayWritable rowBatchBytes = reader.getColumn(columnIndex, null);
          for (int rowIndex = 0; rowIndex < rowBatchBytes.size(); rowIndex++) {
            incrementNumRecords();
            Record outputRecord = record.copy();
            BytesRefWritable rowBytes = rowBatchBytes.get(rowIndex);
            outputRecord.put(rcColumn.getOutputField(), updateColumnValue(rcColumn, rowBytes));
            
            // pass record to next command in chain:
            if (!getChild().process(outputRecord)) {
              return false;
            }
          }
        }
      }
      return true;
    }

    private Writable updateColumnValue(RCFileColumn column, BytesRefWritable bytesRef) throws IOException {
      Writable newColumnValue = column.newWritable();
      // Small optimization to bypass DataInput read if the column writable is
      // BytesRefWritable
      if (newColumnValue.getClass() == BytesRefWritable.class) {
        newColumnValue = bytesRef;
      } else {
        byte[] currentRowBytes = Arrays.copyOfRange(bytesRef.getData(),
            bytesRef.getStart(), bytesRef.getStart() + bytesRef.getLength());
        DataInput dataInput = ByteStreams.newDataInput(currentRowBytes);
        newColumnValue.readFields(dataInput);
      }
      return newColumnValue;
    }

    
    // /////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    // /////////////////////////////////////////////////////////////////////////////
    private enum RCFileReadMode {
      column, row
    }

    
    // /////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    // /////////////////////////////////////////////////////////////////////////////
    private static final class RCFileColumn {
      
      private final String outputField;
      private final Class<Writable> writableClass;
      private final Configuration conf;

      private RCFileColumn(String outputField, Class<Writable> writableClass, Configuration conf) {
        this.outputField = outputField;
        this.writableClass = writableClass;
        this.conf = conf;
      }

      private String getOutputField() {
        return outputField;
      }

      public Writable newWritable() {
        return ReflectionUtils.newInstance(writableClass, conf);
      }

    }
  }
}
