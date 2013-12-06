/**
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
package org.kitesdk.data.hbase.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * An AvroRecordBuilderFactory instance that creates an AvroRecordBuilder that
 * constructs GenericRecord types.
 */
public class GenericAvroRecordBuilderFactory implements
    AvroRecordBuilderFactory<GenericRecord> {

  private final Schema recordSchema;

  /**
   * Construct this factory such that it builds GenericRecord AvroRecordBuilders
   * that crate GenericRecords with the specified Schema.
   * 
   * @param recordSchema
   *          The Avro Schema the created AvroRecordBuilders will construct
   *          GenericRecords with.
   */
  public GenericAvroRecordBuilderFactory(Schema recordSchema) {
    this.recordSchema = recordSchema;
  }

  /**
   * The AvroRecordBuilder implementation that will build GenericRecords.
   */
  private static class GenericAvroRecordBuilder implements
      AvroRecordBuilder<GenericRecord> {

    private final GenericRecord record;

    public GenericAvroRecordBuilder(Schema recordSchema) {
      record = new GenericData.Record(recordSchema);
    }

    @Override
    public void put(String field, Object value) {
      record.put(field, value);
    }

    @Override
    public GenericRecord build() {
      return record;
    }
  }

  /**
   * Get a new AvroRecordBuilder instance.
   * 
   * @return The new AvroRecordBuilder instance.
   */
  @Override
  public AvroRecordBuilder<GenericRecord> getBuilder() {
    return new GenericAvroRecordBuilder(recordSchema);
  }

  /**
   * Returns a Class<GenericRecord> instance since GenericRecordBuilder only
   * creates GenericRecord instances.
   * 
   * @return The GenericRecord class
   */
  @Override
  public Class<GenericRecord> getRecordClass() {
    return GenericRecord.class;
  }
}
