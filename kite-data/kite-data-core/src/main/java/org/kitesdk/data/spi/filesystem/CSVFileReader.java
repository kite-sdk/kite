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

package org.kitesdk.data.spi.filesystem;

import au.com.bytecode.opencsv.CSVReader;
import java.io.InputStream;
import javax.annotation.Nullable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReaderException;
import org.kitesdk.data.spi.AbstractDatasetReader;
import org.kitesdk.data.spi.EntityAccessor;
import org.kitesdk.data.spi.ReaderWriterState;
import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.NoSuchElementException;

public class CSVFileReader<E> extends AbstractDatasetReader<E> {

  private static final Logger LOG = LoggerFactory
      .getLogger(CSVFileReader.class);

  private final CSVProperties props;
  private final FileSystem fs;
  private final Path path;
  private final Schema schema;

  private final Class<E> recordClass;

  private CSVReader reader = null;

  // progress reporting
  private long size = 0;
  private InputStream incoming = null;

  // state
  private ReaderWriterState state = ReaderWriterState.NEW;
  private boolean hasNext = false;
  private String[] next = null;
  private Schema.Field[] fields = null;
  private int[] indexes = null; // Record position to CSV field position

  @SuppressWarnings("unchecked")
  public CSVFileReader(FileSystem fileSystem, Path path,
      DatasetDescriptor descriptor, EntityAccessor<E> accessor) {
    this.fs = fileSystem;
    this.path = path;
    this.schema = accessor.getEntitySchema();
    this.recordClass = accessor.getType();
    this.state = ReaderWriterState.NEW;
    this.props = CSVProperties.fromDescriptor(descriptor);

    Preconditions.checkArgument(Schema.Type.RECORD.equals(schema.getType()),
        "Schemas for CSV files must be records of primitive types");
  }

  public CSVFileReader(InputStream incoming, CSVProperties props,
                       Schema schema, Class<E> type) {
    this.fs = null;
    this.path = null;
    this.incoming = incoming;
    this.schema = schema;
    this.recordClass = type;
    this.state = ReaderWriterState.NEW;
    this.props = props;

    Preconditions.checkArgument(Schema.Type.RECORD.equals(schema.getType()),
        "Schemas for CSV files must be records of primitive types");
  }

  @Override
  @SuppressWarnings("unchecked")
  public void initialize() {
    Preconditions.checkState(state.equals(ReaderWriterState.NEW),
        "A reader may not be opened more than once - current state:%s", state);

    if (incoming != null) {
      this.size = 1; // avoid divide by 0 errors
    } else {
      Preconditions.checkNotNull(fs, "FileSystem cannot be null");
      Preconditions.checkNotNull(path, "Path cannot be null");
      try {
        this.incoming = fs.open(path);
        this.size = fs.getFileStatus(path).getLen();
      } catch (IOException ex) {
        throw new DatasetReaderException("Cannot open path: " + path, ex);
      }
    }

    this.reader = CSVUtil.newReader(incoming, props);

    if (props.useHeader) {
      this.hasNext = advance();
    }

    // initialize the index and field arrays
    fields = schema.getFields().toArray(new Schema.Field[schema.getFields().size()]);
    indexes = new int[fields.length];

    if (next != null) {
      for (int i = 0; i < fields.length; i += 1) {
        fields[i] = schema.getFields().get(i);
        indexes[i] = Integer.MAX_VALUE; // never present in the row
      }

      // there's a header in next
      for (int i = 0; i < next.length; i += 1) {
        Schema.Field field = schema.getField(next[i]);
        if (field != null) {
          indexes[field.pos()] = i;
        }
      }

    } else {
      // without a header, map to fields by position
      for (int i = 0; i < fields.length; i += 1) {
        fields[i] = schema.getFields().get(i);
        indexes[i] = i;
      }
    }

    // initialize by reading the first record
    this.hasNext = advance();

    this.state = ReaderWriterState.OPEN;
  }

  @Override
  public boolean hasNext() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to read from a file in state:%s", state);
    return hasNext;
  }

  @Override
  public E next() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to read from a file in state:%s", state);

    if (!hasNext) {
      throw new NoSuchElementException();
    }

    try {
      E record = makeRecord();
      return record;
    } finally {
      this.hasNext = advance();
    }
  }

  private boolean advance() {
    try {
      next = reader.readNext();
    } catch (IOException ex) {
      throw new DatasetReaderException("Could not read record", ex);
    }
    return (next != null);
  }

  @Override
  public void close() {
    if (!state.equals(ReaderWriterState.OPEN)) {
      return;
    }

    LOG.debug("Closing reader on path:{}", path);

    try {
      reader.close();
    } catch (IOException e) {
      throw new DatasetReaderException("Unable to close reader path:" + path, e);
    }

    state = ReaderWriterState.CLOSED;
  }

  @Override
  public boolean isOpen() {
    return (this.state == ReaderWriterState.OPEN);
  }

  private E makeRecord() {
    if (recordClass != GenericData.Record.class && !recordClass.isInterface()) {
      E record = makeReflectRecord();
      if (record != null) {
        return record;
      }
    }
    return makeGenericRecord();
  }

  @SuppressWarnings("unchecked")
  private E makeGenericRecord() {
    GenericRecord record = new GenericData.Record(schema);
    fillIndexed(record, next);
    return (E) record;
  }

  @SuppressWarnings("unchecked")
  private E makeReflectRecord() {
    E record = (E) ReflectData.get().newInstance(recordClass, schema);
    if (record instanceof IndexedRecord) {
      fillIndexed((IndexedRecord) record, next);
    } else {
      fillReflect(record, next);
    }
    return record;
  }

  private void fillIndexed(IndexedRecord record, String[] data) {
    for (int i = 0; i < indexes.length; i += 1) {
      int index = indexes[i];
      record.put(i,
          makeValue(index < data.length ? data[index] : null, fields[i]));
    }
  }

  private void fillReflect(Object record, String[] data) {
    for (int i = 0; i < indexes.length; i += 1) {
      Schema.Field field = fields[i];
      int index = indexes[i];
      Object value = makeValue(index < data.length ? data[index] : null, field);
      ReflectData.get().setField(record, field.name(), i, value);
    }
  }

  private static Object makeValue(@Nullable String string, Schema.Field field) {
    Object value = makeValue(string, field.schema());
    if (value != null || nullOk(field.schema())) {
      return value;
    } else {
      // this will fail if there is no default value
      return ReflectData.get().getDefaultValue(field);
    }
  }

  /**
   * Returns a the value as the first matching schema type or null.
   *
   * Note that if the value may be null even if the schema does not allow the
   * value to be null.
   *
   * @param string a String representation of the value
   * @param schema a Schema
   * @return the string coerced to the correct type from the schema or null
   */
  private static Object makeValue(@Nullable String string, Schema schema) {
    if (string == null) {
      return null;
    }

    try {
      switch (schema.getType()) {
        case BOOLEAN:
          return Boolean.valueOf(string);
        case STRING:
          return string;
        case FLOAT:
          return Float.valueOf(string);
        case DOUBLE:
          return Double.valueOf(string);
        case INT:
          return Integer.valueOf(string);
        case LONG:
          return Long.valueOf(string);
        case ENUM:
          // TODO: translate to enum class
          if (schema.hasEnumSymbol(string)) {
            return string;
          } else {
            try {
              return schema.getEnumSymbols().get(Integer.valueOf(string));
            } catch (IndexOutOfBoundsException ex) {
              return null;
            }
          }
        case UNION:
          Object value = null;
          for (Schema possible : schema.getTypes()) {
            value = makeValue(string, possible);
            if (value != null) {
              return value;
            }
          }
          return null;
        case NULL:
          return null;
        default:
          // FIXED, BYTES, MAP, ARRAY, RECORD are not supported
          throw new DatasetReaderException(
              "Unsupported field type:" + schema.getType());
      }
    } catch (NumberFormatException e) {
      // empty string is considered null for numeric types
      if (string.isEmpty()) {
        return null;
      } else {
        throw e;
      }
    }
  }

  /**
   * Returns whether null is allowed by the schema.
   *
   * @param schema a Schema
   * @return true if schema allows the value to be null
   */
  private static boolean nullOk(Schema schema) {
    if (Schema.Type.NULL == schema.getType()) {
      return true;
    } else if (Schema.Type.UNION == schema.getType()) {
      for (Schema possible : schema.getTypes()) {
        if (nullOk(possible)) {
          return true;
        }
      }
    }
    return false;
  }

  public RecordReader<E, Void> asRecordReader() {
    Preconditions.checkArgument(incoming instanceof FSDataInputStream,
        "Cannot use {} in a record reader", incoming.getClass());
    return new CSVRecordReader();
  }

  public class CSVRecordReader extends RecordReader<E, Void> {
    private E current;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (hasNext()) {
        this.current = next();
        return true;
      }
      return false;
    }

    @Override
    public E getCurrentKey() throws IOException, InterruptedException {
      return current;
    }

    @Override
    public Void getCurrentValue() throws IOException, InterruptedException {
      return null;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return ((float) ((FSDataInputStream) incoming).getPos()) / size;
    }

    @Override
    public void close() throws IOException {
      CSVFileReader.this.close();
    }
  }
}
