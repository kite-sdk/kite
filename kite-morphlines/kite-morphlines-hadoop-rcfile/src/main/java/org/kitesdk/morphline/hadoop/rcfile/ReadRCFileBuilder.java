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
package org.kitesdk.morphline.hadoop.rcfile;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Configs;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Validator;
import org.kitesdk.morphline.stdio.AbstractParser;

import com.google.common.io.ByteStreams;
import com.typesafe.config.Config;

/**
 * Command for reading Record Columnar (RC) Files. RCFile can be read in two
 * ways
 *
 * 1. Row Wise - Emits one record for every row in the RCFile, with columns
 * mapped with names as provided in the column map
 *
 * 2. Column Wise - For every row split (block) in the RC File, Emits one record
 * for each column with all the row values for that column as a List
 *
 * For a RCFile with n rows and m columns. Row wise would emit n records and
 * Column wise would emit m * (row splits) records.
 * De-Compression is handled automatically
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
    
    private final List<RCFileColumn> columns = new ArrayList();
    private final RCFileReadMode readMode;
    private final boolean includeMetaData;
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
      
      for (Config columnConfig : getConfigs().getConfigList(config, "columns")) {
        columns.add(new RCFileColumn(columnConfig, conf));
      }
      validateArguments();
    }

    @Override
    protected boolean doProcess(Record record, InputStream in) throws IOException {
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
        default :
          throw new IllegalStateException();
        }
      } catch (IOException e) {
        throw new MorphlineRuntimeException("IOException while processing attachment "
                + attachmentPath.getName(), e);
      } finally {
        if (reader != null) {
          reader.close();
        }
      }
    }

    private Path getAttachmentPath(Record record) {
      // We have more meaningful RCFile error messages if we have an attachment name
      String attachmentName = (String) record.getFirstValue(Fields.ATTACHMENT_NAME);
      if (attachmentName == null) {
        attachmentName = "UNKNOWN";
      }
      return new Path(STREAM_PROTOCOL + attachmentName);
    }

    private boolean readRowWise(final RCFile.Reader reader, final Record record)
        throws IOException {
      
      LongWritable rowID = new LongWritable();      

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
        BytesRefArrayWritable rowBatchBytes = new BytesRefArrayWritable();
        rowBatchBytes.resetValid(columns.size());
        reader.getCurrentRow(rowBatchBytes);

        // Read all the columns configured and set it in the output record
        for (RCFileColumn rcColumn : columns) {
          BytesRefWritable columnBytes = rowBatchBytes.get(rcColumn.getInputField());
          outputRecord.put(rcColumn.getOutputField(), updateColumnValue(rcColumn, columnBytes));
        }
        
        // pass record to next command in chain:
        if (!getChild().process(outputRecord)) {
          return false;
        }
      }
      return true;
    }

    private boolean readColumnWise(RCFile.Reader reader, Record record)
        throws IOException {

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

        for (RCFileColumn rcColumn : columns) {
          BytesRefArrayWritable rowBatchBytes = reader.getColumn(rcColumn.getInputField(), null);
          List<Writable> rowValues = new ArrayList<Writable>(rowBatchBytes.size());
          incrementNumRecords();
          Record outputRecord = record.copy();
          for (int rowIndex = 0; rowIndex < rowBatchBytes.size(); rowIndex++) {
            BytesRefWritable rowBytes = rowBatchBytes.get(rowIndex);
            rowValues.add(updateColumnValue(rcColumn, rowBytes));
          }
          outputRecord.put(rcColumn.getOutputField(), rowValues);
          
          // pass record to next command in chain:
          if (!getChild().process(outputRecord)) {
            return false;
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
    private static enum RCFileReadMode {
      column, row
    }

    
    // /////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    // /////////////////////////////////////////////////////////////////////////////
    private static final class RCFileColumn {
      
      private final int inputField;
      private final String outputField;
      private final Class<Writable> writableClass;
      private final Configuration conf;

      public RCFileColumn(Config columnConfig, Configuration conf) {
        this.conf = conf;
        Configs configs = new Configs();
        this.inputField = configs.getInt(columnConfig, "inputField");
        if (inputField < 0) {
          throw new MorphlineCompilationException(
              "Invalid column inputField specified: " + inputField, columnConfig);
        }

        this.outputField = configs.getString(columnConfig, "outputField");
        String writableClassString = configs.getString(columnConfig, "writableClass");

        if (writableClassString == null || writableClassString.isEmpty()) {
          throw new MorphlineCompilationException(
              "No writableClass specified for column " + outputField, columnConfig);
        }
        try {
          Class clazz = Class.forName(writableClassString);
          if (!Writable.class.isAssignableFrom(clazz)) {
            throw new MorphlineCompilationException("writableClass provided "
                + writableClassString + " for column " + outputField
                + " does not implement " + Writable.class.getName(), columnConfig);
          }
          this.writableClass = clazz;
        } catch (ClassNotFoundException e) {
          throw new MorphlineCompilationException("Could not load class "
              + writableClassString + " definition", columnConfig, e);
        }
        configs.validateArguments(columnConfig);
      }

      public int getInputField() {
        return inputField;
      }

      public String getOutputField() {
        return outputField;
      }

      public Writable newWritable() {
        return ReflectionUtils.newInstance(writableClass, conf);
      }

    }
  }
}
