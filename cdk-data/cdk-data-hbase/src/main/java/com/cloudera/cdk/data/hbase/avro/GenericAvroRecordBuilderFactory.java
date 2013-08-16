// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase.avro;

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