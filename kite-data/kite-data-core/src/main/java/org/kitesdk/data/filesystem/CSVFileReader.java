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

package org.kitesdk.data.filesystem;

import au.com.bytecode.opencsv.CSVReader;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReaderException;
import org.kitesdk.data.spi.AbstractDatasetReader;
import org.kitesdk.data.spi.ReaderWriterState;
import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.NoSuchElementException;

class CSVFileReader<E> extends AbstractDatasetReader<E> {

  private static final Logger logger = LoggerFactory
      .getLogger(CSVFileReader.class);

  public static final String CHARSET_PROPERTY = "dataset.csv.charset";
  public static final String DELIMITER_PROPERTY = "dataset.csv.delimiter";
  public static final String QUOTE_CHAR_PROPERTY = "dataset.csv.quote-char";
  public static final String ESCAPE_CHAR_PROPERTY = "dataset.csv.escape-char";
  public static final String LINES_TO_SKIP_PROPERTY = "dataset.csv.lines-to-skip";

  // old properties
  public static final String OLD_CHARSET_PROPERTY = "cdk.csv.charset";
  public static final String OLD_DELIMITER_PROPERTY = "cdk.csv.delimiter";
  public static final String OLD_QUOTE_CHAR_PROPERTY = "cdk.csv.quote-char";
  public static final String OLD_ESCAPE_CHAR_PROPERTY = "cdk.csv.escape-char";
  public static final String OLD_LINES_TO_SKIP_PROPERTY = "cdk.csv.lines-to-skip";

  public static final String DEFAULT_CHARSET = "utf8";
  public static final String DEFAULT_DELIMITER = ",";
  public static final String DEFAULT_QUOTE = "\"";
  public static final String DEFAULT_ESCAPE = "\\";
  public static final int DEFAULT_LINES_TO_SKIP = 0;

  private final FileSystem fs;
  private final Path path;
  private final Schema schema;

  // configuration
  private final String charset;
  private final String delimiter;
  private final String quote;
  private final String escape;
  private final int linesToSkip;
  private Class<E> recordClass = null;

  private CSVReader reader = null;

  // state
  private ReaderWriterState state = ReaderWriterState.NEW;
  private boolean hasNext = false;
  private String[] next = null;

  public CSVFileReader(FileSystem fileSystem, Path path, DatasetDescriptor descriptor) {
    this.fs = fileSystem;
    this.path = path;
    this.schema = descriptor.getSchema();
    this.state = ReaderWriterState.NEW;

    Schema schema = descriptor.getSchema();
    Preconditions.checkArgument(Schema.Type.RECORD.equals(schema.getType()),
        "Schemas for CSV files must be records of primitive types");

    this.charset = coalesce(
        descriptor.getProperty(CHARSET_PROPERTY),
        descriptor.getProperty(OLD_CHARSET_PROPERTY),
        DEFAULT_CHARSET);
    this.delimiter= coalesce(
        descriptor.getProperty(DELIMITER_PROPERTY),
        descriptor.getProperty(OLD_DELIMITER_PROPERTY),
        DEFAULT_DELIMITER);
    this.quote = coalesce(
        descriptor.getProperty(QUOTE_CHAR_PROPERTY),
        descriptor.getProperty(OLD_QUOTE_CHAR_PROPERTY),
        DEFAULT_QUOTE);
    this.escape = coalesce(
        descriptor.getProperty(ESCAPE_CHAR_PROPERTY),
        descriptor.getProperty(OLD_ESCAPE_CHAR_PROPERTY),
        DEFAULT_ESCAPE);
    final String linesToSkipString = coalesce(
        descriptor.getProperty(LINES_TO_SKIP_PROPERTY),
        descriptor.getProperty(OLD_LINES_TO_SKIP_PROPERTY));
    int lines = DEFAULT_LINES_TO_SKIP;
    if (linesToSkipString != null) {
      try {
        lines = Integer.valueOf(linesToSkipString);
      } catch (NumberFormatException ex) {
        logger.debug("Defaulting lines to skip, failed to parse: {}",
            linesToSkipString);
        // lines remains set to the default
      }
    }
    this.linesToSkip = lines;
  }

  /**
   * Returns the first non-null value from the sequence or null if there is no
   * non-null value.
   */
  private static <T> T coalesce(T... values) {
    for (T value : values) {
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void open() {
    Preconditions.checkState(state.equals(ReaderWriterState.NEW),
        "A reader may not be opened more than once - current state:%s", state);

    // may be null if not using specific records
    this.recordClass = SpecificData.get().getClass(schema);

    FSDataInputStream incoming;
    try {
      incoming =  fs.open(path);
    } catch (IOException ex) {
      throw new DatasetReaderException("Cannot open path: " + path, ex);
    }

    this.reader = new CSVReader(
        new InputStreamReader(incoming, Charset.forName(charset)),
        delimiter.charAt(0), quote.charAt(0), escape.charAt(0), linesToSkip,
        false /* strict quotes off: don't ignore unquoted strings */,
        true /* ignore leading white-space */ );

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

    logger.debug("Closing reader on path:{}", path);

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
    if (recordClass != null) {
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
      final Schema.Field field = schema.getFields().get(i);
      final Object value = makeValue(i < data.length ? data[i] : null, field);
      try {
        final PropertyDescriptor propertyDescriptor = new PropertyDescriptor(
                field.name(), record.getClass(), null, setter(field.name()));
        propertyDescriptor.getWriteMethod().invoke(record, value);
      } catch (IntrospectionException ex) {
        throw new IllegalStateException("Cannot set property " + field.name() +
            " on " + record.getClass().getName(), ex);
      } catch (InvocationTargetException ex) {
        throw new IllegalStateException("Cannot set property " + field.name() +
            " on " + record.getClass().getName(), ex);
      } catch (IllegalAccessException ex) {
        throw new IllegalStateException("Cannot set property " + field.name() +
            " on " + record.getClass().getName(), ex);
      }
    }
  }

  private static String setter(String name) {
    return "set" +
        name.substring(0, 1).toUpperCase(Locale.ENGLISH) +
        name.substring(1);
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
}
