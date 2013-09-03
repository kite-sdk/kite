// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase.avro;

import org.apache.avro.Schema;

import com.cloudera.cdk.data.hbase.KeySchema;

/**
 * A KeySchema implementation powered by Avro.
 */
public class AvroKeySchema extends KeySchema {

  private final Schema schema;

  /**
   * Constructor for the AvroKeySchema.
   * 
   * @param schema
   *          The Avro Schema that underlies this KeySchema implementation
   * @param rawSchema
   *          The Avro Schema as a string that underlies the KeySchema
   *          implementation
   */
  public AvroKeySchema(Schema schema, String rawSchema) {
    super(rawSchema);
    this.schema = schema;
  }

  /**
   * Get the Avro Schema that underlies this KeySchema implementation.
   * 
   * @return The Avro Schema
   */
  public Schema getAvroSchema() {
    return schema;
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((schema == null) ? 0 : schema.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    AvroKeySchema other = (AvroKeySchema) obj;
    if (schema == null) {
      if (other.schema != null)
        return false;
    } else if (!schema.equals(other.schema))
      return false;
    return true;
  }
}
