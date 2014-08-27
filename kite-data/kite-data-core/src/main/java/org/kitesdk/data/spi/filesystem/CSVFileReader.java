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

class CSVFileReader<E> extends AbstractDatasetReader<E> {

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
  private FSDataInputStream incoming = null;

  // state
  private ReaderWriterState state = ReaderWriterState.NEW;
  private boolean hasNext = false;
  private String[] next = null;

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

  @Override
  @SuppressWarnings("unchecked")
  public void initialize() {
    Preconditions.checkState(state.equals(ReaderWriterState.NEW),
        "A reader may not be opened more than once - current state:%s", state);

    try {
      this.incoming =  fs.open(path);
      this.size = fs.getFileStatus(path).getLen();
    } catch (IOException ex) {
      throw new DatasetReaderException("Cannot open path: " + path, ex);
    }

    this.reader = CSVUtil.newReader(incoming, props);
    // header is determined by the schema, so skip the file header
    // TODO: support the orderByHeader property
    if (props.useHeader) {
      this.hasNext = advance();
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
      fillReflect(record, next, schema);
    }
    return record;
  }

  private static void fillIndexed(IndexedRecord record, String[] data) {
    Schema schema = record.getSchema();
    for (int i = 0, n = schema.getFields().size(); i < n; i += 1) {
      final Schema.Field field = schema.getFields().get(i);
      if (i < data.length) {
        record.put(i, makeValue(data[i], field));
      } else {
        record.put(i, makeValue(null, field));
      }
    }
  }

  private static void fillReflect(Object record, String[] data, Schema schema) {
    for (int i = 0, n = schema.getFields().size(); i < n; i += 1) {
      Schema.Field field = schema.getFields().get(i);
      Object value = makeValue(i < data.length ? data[i] : null, field);
      ReflectData.get().setField(record, field.name(), i, value);
    }
  }

  private static Object makeValue(String string, Schema.Field field) {
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
  private static Object makeValue(String string, Schema schema) {
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
    } catch (NumberFormatException ex) {
      return null;
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
      return ((float) incoming.getPos()) / size;
    }

    @Override
    public void close() throws IOException {
      CSVFileReader.this.close();
    }
  }
}
