// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

/**
 * Interface for the HBase Common Key and Entity parser.
 * 
 * @param <RAW_SCHEMA>
 *          The type that the key and entities get parsed from
 * @param <KEY_SCHEMA>
 *          The type that the key schema gets parsed to. Extends
 *          KeySchema<RAW_SCHEMA>
 * @param <ENTITY_SCHEMA>
 *          The type that the raw entity schema gets parsed to. Extends
 *          EntitySchema<RAW_SCHEMA>
 */
public interface KeyEntitySchemaParser<RAW_SCHEMA, KEY_SCHEMA extends KeySchema<RAW_SCHEMA>, ENTITY_SCHEMA extends EntitySchema<RAW_SCHEMA>> {

  /**
   * Parse the raw entity schema.
   * 
   * @param schema
   *          The raw entity schema
   * @return The parsed schema
   */
  public ENTITY_SCHEMA parseEntity(RAW_SCHEMA schema);

  /**
   * Parse the raw key schema
   * 
   * @param schema
   *          The raw key schema
   * @return The parsed schema
   */
  public KEY_SCHEMA parseKey(RAW_SCHEMA schema);
}
