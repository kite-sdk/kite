// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

import com.cloudera.cdk.data.dao.EntitySchema;
import com.cloudera.cdk.data.dao.KeySchema;

/**
 * Interface for the HBase Common Key and Entity parser.
 *
 * @param <KEY_SCHEMA>
 *          The type that the key schema gets parsed to. Extends
 *          KeySchema<RAW_SCHEMA>
 * @param <ENTITY_SCHEMA>
 *          The type that the raw entity schema gets parsed to. Extends
 *          EntitySchema<RAW_SCHEMA>
 */
public interface KeyEntitySchemaParser<KEY_SCHEMA extends KeySchema, ENTITY_SCHEMA extends EntitySchema> {

  /**
   * Parse the raw entity schema.
   * 
   * @param schema
   *          The raw entity schema
   * @return The parsed schema
   */
  public ENTITY_SCHEMA parseEntity(String schema);

  /**
   * Parse the raw key schema
   * 
   * @param schema
   *          The raw key schema
   * @return The parsed schema
   */
  public KEY_SCHEMA parseKey(String schema);
}
