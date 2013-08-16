// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase.avro;

import java.util.Collection;

import org.apache.avro.Schema;

import com.cloudera.cdk.data.hbase.EntitySchema;

/**
 * An EntitySchema implementation powered by Avro.
 */
public class AvroEntitySchema extends EntitySchema<String> {

  private final Schema schema;

  /**
   * Constructor for the AvroEntitySchema.
   * 
   * @param tables
   *          The tables this EntitySchema can be persisted to
   * @param schema
   *          The Avro Schema that underlies this EntitySchema implementation
   * @param rawSchema
   *          The Avro Schema as a string that underlies the EntitySchema
   *          implementation
   * @param fieldMappings
   *          The list of FieldMappings that specify how each field maps to an
   *          HBase row
   * @param isTransactional
   *          Specifies whether this entity participates in transactions
   */
  public AvroEntitySchema(Collection<String> tables, Schema schema, String rawSchema,
      Collection<FieldMapping> fieldMappings, boolean isTransactional) {
    super(tables, rawSchema, fieldMappings, isTransactional);
    this.schema = schema;
  }

  /**
   * Get the Avro Schema that underlies this EntitySchema implementation.
   * 
   * @return The Avro Schema
   */
  public Schema getAvroSchema() {
    return schema;
  }
}
