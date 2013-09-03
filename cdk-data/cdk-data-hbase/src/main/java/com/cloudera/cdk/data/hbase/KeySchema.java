// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

/**
 * The KeySchema type.
 */
public class KeySchema {

  private final String rawSchema;

  /**
   * @param rawSchema
   *          The raw schema
   */
  public KeySchema(String rawSchema) {
    this.rawSchema = rawSchema;
  }

  /**
   * Get the raw schema
   * 
   * @return The raw schema.
   */
  public String getRawSchema() {
    return rawSchema;
  }
}
