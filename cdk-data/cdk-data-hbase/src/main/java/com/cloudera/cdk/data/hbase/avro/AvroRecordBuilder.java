// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase.avro;

/**
 * An builder interface used to construct Avro records. The Avro entity mapper
 * types must create Avro records, of which there are multiple classes. One
 * class of Avro record is GenericRecord. Another is SpecificRecord. These are
 * constructed in different ways.
 * 
 * For each class of avro record supported by the entity mappers, there should
 * be an AvroRecordBuilder implementation.
 * 
 * @param <T>
 *          The type of record to build.
 */
public interface AvroRecordBuilder<T> {

  /**
   * Add a value to a field of an avro record.
   * 
   * @param field
   *          The field to add a value for.
   * @param value
   *          The value to add.
   */
  public void put(String field, Object value);

  /**
   * Build the avro record.
   * 
   * @return The constructed avro record.
   */
  public T build();
}