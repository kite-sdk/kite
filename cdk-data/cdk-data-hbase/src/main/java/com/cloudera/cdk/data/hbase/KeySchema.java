// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

/**
 * The KeySchema type.
 * 
 * @param <RAW_SCHEMA>
 *          The type that gets parsed by the KeyEntitySchemaParser
 */
public class KeySchema<RAW_SCHEMA> {

  private final RAW_SCHEMA rawSchema;

  /**
   * @param rawSchema
   *          The raw schema
   */
  public KeySchema(RAW_SCHEMA rawSchema) {
    this.rawSchema = rawSchema;
  }

  /**
   * Get the raw schema
   * 
   * @return The raw schema.
   */
  public RAW_SCHEMA getRawSchema() {
    return rawSchema;
  }
}
