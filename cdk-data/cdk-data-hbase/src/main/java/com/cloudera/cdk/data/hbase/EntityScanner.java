// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

/**
 * A Scanner interface that represents an Iterable that allows us to iterate
 * over entities in an HBase table, returning them as KeyEntity instances.
 * 
 * @param <K>
 *          The underlying key record type
 * @param <E>
 *          The type of the entity to return
 */
public interface EntityScanner<K, E> extends Iterable<KeyEntity<K, E>> {

  /**
   * Opens the scanner over the table, with scan parameters.
   */
  public void open();

  /**
   * Closes the entity scanner, and cleans up any underlying resources.
   */
  public void close();
}
