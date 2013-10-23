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
public class ReadRCFileBuilder implements CommandBuilder {
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
    public static final String CONFIG_READ_MODE = "readMode";
    public static final String CONFIG_COLUMN_MAP = "columnMap";
    public static final String COLUMN_INDEX = "index";
    public static final String CONFIG_FIELD_NAME = "fieldName";
    public static final String CONFIG_WRITABLE_CLASS = "writableClass";
    public static final String CONFIG_INCLUDE_META_DATA = "includeMetaData";
    private static final Object STREAM_PROTOCOL = "stream://";

    private final boolean includeMetaData;
    private final RCFileReadMode readMode;
    private Map<Integer, RCFileColumn> columnMap;

    public ReadRCFile(CommandBuilder builder, Config config, Command parent,
        Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);
      this.includeMetaData = getConfigs().getBoolean(config,
          CONFIG_INCLUDE_META_DATA, false);
      this.readMode = RCFileReadMode.valueOf(getConfigs().getString(config,
          CONFIG_READ_MODE, RCFileReadMode.row.name()));
      this.columnMap = parseColumnMap(config);
      validateArguments();
    }

    private Map<Integer, RCFileColumn> parseColumnMap(final Config config) {
      Configuration conf = new Configuration();
      List<? extends Config> columnMapConfigList = getConfigs().getConfigList(
          config, CONFIG_COLUMN_MAP);
      Map<Integer, RCFileColumn> rcFileColumnMap = Maps.newHashMap();
      for (Config columnMapConfig : columnMapConfigList) {
        int index = getConfigs().getInt(columnMapConfig, COLUMN_INDEX);
        String fieldName = getConfigs().getString(columnMapConfig,
            CONFIG_FIELD_NAME);
        String writableClassString = getConfigs().getString(columnMapConfig,
            CONFIG_WRITABLE_CLASS, BytesRefWritable.class.getName());

        Class<Writable> writableClass;
        try {
          writableClass = (Class<Writable>) Class.forName(writableClassString);
        } catch (ClassNotFoundException e) {
          throw new MorphlineCompilationException("Could not load class "
              + writableClassString + " definition", config, e);
        }
        rcFileColumnMap.put(index, new RCFileColumn(fieldName, writableClass,
            conf));
      }
      return ImmutableMap.copyOf(rcFileColumnMap);
    }

    @Override
    protected boolean doProcess(Record record, InputStream in)
        throws IOException {
      Configuration conf = new Configuration();
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

        LOG.debug("Reading row " + rowID.get());
        BytesRefArrayWritable rowBatchBytes = new BytesRefArrayWritable();
        rowBatchBytes.resetValid(columnMap.size());
        reader.getCurrentRow(rowBatchBytes);
        Record outputRecord = prepareOutputRecord(record, metadata);
        // Read all the columns configured and set it in the output record
        for (Integer columnIndex : columnMap.keySet()) {
          RCFileColumn rcColumn = columnMap.get(columnIndex);
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
      for (Integer columnIndex : columnMap.keySet()) {
        RCFileColumn rcColumn = columnMap.get(columnIndex);
        LOG.debug("Reading column " + rcColumn.getFieldName());
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
          for (Integer rowIndex = 0; rowIndex < rowBatchBytes.size(); rowIndex++) {
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
    private class RCFileColumn {
      private String fieldName;
      private Writable writable;
      private Class<Writable> writableClass;
      private Configuration conf;

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
