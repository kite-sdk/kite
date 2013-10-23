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
import java.util.List;
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
import com.cloudera.cdk.morphline.base.Fields;
import com.cloudera.cdk.morphline.base.Validator;
import com.cloudera.cdk.morphline.stdio.AbstractParser;
import com.google.common.collect.ImmutableMap;
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
  public Command build(Config config, Command parent, Command child,
      MorphlineContext context) {
    return new ReadRCFile(this, config, parent, child, context);
  }

  // /////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  // /////////////////////////////////////////////////////////////////////////////
  private static final class ReadRCFile extends AbstractParser {
    private static final Object STREAM_PROTOCOL = "stream://";

    private final boolean includeMetaData;
    private final RCFileReadMode readMode;
    private final Map<Integer, RCFileColumn> columnMap;
    private final Configuration conf = new Configuration();

    public ReadRCFile(CommandBuilder builder, Config config, Command parent,
        Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);
      this.includeMetaData = getConfigs().getBoolean(config, "includeMetaData",
          false);
      this.readMode = new Validator<RCFileReadMode>()
          .validateEnum(
              config,
              getConfigs().getString(config, "readMode",
                  RCFileReadMode.row.name()), RCFileReadMode.class);
      this.columnMap = parseColumnMap(config);
      validateArguments(config);
    }

    private Map<Integer, RCFileColumn> parseColumnMap(final Config config) {
      List<? extends Config> columnMapConfigList = getConfigs().getConfigList(
          config, "columns");
      Map<Integer, RCFileColumn> rcFileColumnMap = Maps.newHashMap();
      for (Config columnMapConfig : columnMapConfigList) {
        int inputField = getConfigs().getInt(columnMapConfig, "inputField");
        String fieldName = getConfigs().getString(columnMapConfig, "outputField");
        String writableClassString = getConfigs().getString(columnMapConfig,
            "writableClass");

        if (writableClassString == null || writableClassString.isEmpty()) {
          throw new MorphlineCompilationException(
              "No writableClass specified for column " + fieldName, config);
        }
        Class<Writable> writableClass;
        try {
          Class clazz = Class.forName(writableClassString);
          if (!Writable.class.isAssignableFrom(clazz)) {
            throw new MorphlineCompilationException("writableClass provided "
                + writableClassString + " for column " + fieldName
                + " does not implement " + Writable.class.getName(), config);
          }
          writableClass = (Class<Writable>) clazz;
        } catch (ClassNotFoundException e) {
          throw new MorphlineCompilationException("Could not load class "
              + writableClassString + " definition", config, e);
        }
        rcFileColumnMap.put(inputField, new RCFileColumn(fieldName, writableClass,
            conf));
      }
      return ImmutableMap.copyOf(rcFileColumnMap);
    }

    public void validateArguments(Config config) {
      validateArguments();
      // check if all column map index are > 0
      for (Integer columnIndex : columnMap.keySet()) {
        if (columnIndex < 0) {
          throw new MorphlineCompilationException(
              "Invalid column map index specified " + columnIndex, config);
        }
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
      // We have meaningful references in RCFile Errors
      // if we have a attachment name
      String attachmentName = (String) record
          .getFirstValue(Fields.ATTACHMENT_NAME);
      if (attachmentName == null) {
        attachmentName = record.toString();
      }
      return new Path(STREAM_PROTOCOL + attachmentName);
    }

    private boolean readRowWise(final RCFile.Reader reader, final Record record)
        throws IOException {
      LongWritable rowID = new LongWritable();
      SequenceFile.Metadata metadata = reader.getMetadata();

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

        BytesRefArrayWritable rowBatchBytes = new BytesRefArrayWritable();
        rowBatchBytes.resetValid(columnMap.size());
        reader.getCurrentRow(rowBatchBytes);
        Record outputRecord = prepareOutputRecord(record, metadata);
        // Read all the columns configured and set it in the output record
        for (Map.Entry<Integer, RCFileColumn> columnMapEntry : columnMap
            .entrySet()) {
          RCFileColumn rcColumn = columnMapEntry.getValue();
          Integer columnIndex = columnMapEntry.getKey();
          BytesRefWritable columnBytes = rowBatchBytes.get(columnIndex);
          updateColumnValue(columnIndex, columnBytes);
          outputRecord.put(rcColumn.getFieldName(), rcColumn.getWritable());
        }
        // pass record to next command in chain:
        if (!getChild().process(outputRecord)) {
          return false;
        }
      }
      return true;
    }

    private boolean readColumnWise(final RCFile.Reader reader,
        final Record record) throws IOException {
      SequenceFile.Metadata metadata = reader.getMetadata();
      for (Map.Entry<Integer, RCFileColumn> columnMapEntry : columnMap
          .entrySet()) {
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

          BytesRefArrayWritable rowBatchBytes = reader.getColumn(columnIndex,
              null);
          for (int rowIndex = 0; rowIndex < rowBatchBytes.size(); rowIndex++) {
            Record outputRecord = prepareOutputRecord(record, metadata);
            BytesRefWritable rowBytes = rowBatchBytes.get(rowIndex);
            updateColumnValue(columnIndex, rowBytes);
            outputRecord.put(rcColumn.getFieldName(), rcColumn.getWritable());
            // pass record to next command in chain:
            if (!getChild().process(outputRecord)) {
              return false;
            }
          }
        }
      }
      return true;
    }

    private void updateColumnValue(final Integer columnIndex,
        final BytesRefWritable bytesRef) throws IOException {
      Writable newColumnValue = columnMap.get(columnIndex).newWritable();
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
      columnMap.get(columnIndex).setWritable(newColumnValue);
    }

    private Record prepareOutputRecord(final Record record,
        final SequenceFile.Metadata metadata) {
      incrementNumRecords();
      Record outputRecord = record.copy();
      // All the columns defined has been read for this row
      outputRecord.put(Fields.ATTACHMENT_MIME_TYPE, OUTPUT_MEDIA_TYPE);
      if (includeMetaData && metadata != null) {
        outputRecord.put(RC_FILE_META_DATA, metadata);
      }
      return outputRecord;
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
      private final String fieldName;
      private final Class<Writable> writableClass;
      private final Configuration conf;
      private Writable writable;

      private RCFileColumn(final String fieldName,
          final Class<Writable> writableClass, final Configuration conf) {
        this.fieldName = fieldName;
        this.writableClass = writableClass;
        this.conf = conf;
      }

      private String getFieldName() {
        return fieldName;
      }

      public Writable newWritable() {
        return ReflectionUtils.newInstance(writableClass, conf);
      }

      private Writable getWritable() {
        return writable;
      }

      private void setWritable(final Writable writable) {
        this.writable = writable;
      }
    }
  }
}
