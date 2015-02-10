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

import au.com.bytecode.opencsv.CSVWriter;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.compat.Hadoop;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetOperationException;

class CSVAppender<E> implements FileSystemWriter.FileAppender<E> {

  private final CSVProperties props;
  private final FileSystem fs;
  private final Path path;
  private final Schema schema;

  // the actual writer
  private FSDataOutputStream outgoing;
  private CSVWriter writer;

  public CSVAppender(FileSystem fs, Path path, DatasetDescriptor descriptor) {
    this.fs = fs;
    this.path = path;
    this.schema = descriptor.getSchema();
    Preconditions.checkState(schema.getType() == Schema.Type.RECORD,
        "Unsupported schema (not a record): {}", schema);
    this.props = CSVProperties.fromDescriptor(descriptor);
  }

  @Override
  public void open() throws IOException {
    this.outgoing = fs.create(path, true /* overwrite */);
    this.writer = CSVUtil.newWriter(outgoing, props);
  }

  @Override
  public void append(E entity) throws IOException {
    writer.writeNext(shred(entity));
  }

  @Override
  public void close() throws IOException {
    writer.close();
    outgoing.close();
  }

  @Override
  public void cleanup() throws IOException {
    // No cleanup tasks needed
  }

  @Override
  public void flush() throws IOException {
    writer.flush();
    Hadoop.FSDataOutputStream.hflush.invoke(outgoing);
  }

  @Override
  public void sync() throws IOException {
    flush();
    Hadoop.FSDataOutputStream.hsync.invoke(outgoing);
  }

  private String[] shred(E entity) {
    if (entity instanceof IndexedRecord) {
      return shredIndexed((IndexedRecord) entity, schema);
    } else {
      return shredReflect(entity, schema);
    }
  }

  private static String[] shredIndexed(IndexedRecord entity, Schema schema) {
    List<Schema.Field> fields = schema.getFields();
    String[] values = new String[fields.size()];
    for (int i = 0; i < values.length; i += 1) {
      values[i] = valueString(entity.get(i), fields.get(i).schema());
    }
    return values;
  }

  private static <E> String[] shredReflect(E entity, Schema schema) {
    ReflectData reflect = ReflectData.get();
    List<Schema.Field> fields = schema.getFields();
    String[] values = new String[fields.size()];
    for (int i = 0; i < values.length; i += 1) {
      Schema.Field field = fields.get(i);
      values[i] = valueString(
          reflect.getField(entity, field.name(), i), field.schema());
    }
    return values;
  }

  /**
   * Returns a the value as the first matching schema type or null.
   *
   * Note that if the value may be null even if the schema does not allow the
   * value to be null.
   *
   * @param value a value
   * @param schema a Schema
   * @return a String representation of the value according to the Schema type
   */
  private static String valueString(Object value, Schema schema) {
    if (value == null || schema.getType() == Schema.Type.NULL) {
      return null;
    }

    switch (schema.getType()) {
      case BOOLEAN:
      case FLOAT:
      case DOUBLE:
      case INT:
      case LONG:
      case STRING:
        return value.toString();
      case ENUM:
        // serialize as the ordinal from the schema
        return String.valueOf(schema.getEnumOrdinal(value.toString()));
      case UNION:
        int index = ReflectData.get().resolveUnion(schema, value);
        return valueString(value, schema.getTypes().get(index));
      default:
        // FIXED, BYTES, MAP, ARRAY, RECORD are not supported
        throw new DatasetOperationException(
            "Unsupported field type:" + schema.getType());
    }
  }
}
