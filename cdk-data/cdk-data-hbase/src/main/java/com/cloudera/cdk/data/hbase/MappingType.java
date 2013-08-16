// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

/**
 * The supported Mapping Types, which control how an entity field maps to
 * columns in an HBase table.
 */
public enum MappingType {

  // Maps a value to a single column.
  COLUMN,

  // Maps a map or record value to columns
  // in a column family.
  KEY_AS_COLUMN,
  
  // The field will be populated with the
  // current version of the entity. This
  // allows the version to be checked if this
  // same entity is persisted back, to make sure
  // it hasn't changed.
  OCC_VERSION
}
