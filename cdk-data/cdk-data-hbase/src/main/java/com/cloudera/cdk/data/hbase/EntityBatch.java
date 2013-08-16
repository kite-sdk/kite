package com.cloudera.cdk.data.hbase;

public interface EntityBatch<K, E> {

  /**
   * Put the entity into the HBase table with K key. Since this is a part of a
   * batch operation, this entity will not be committed to the table until the
   * writeBuffer has reached capacity.
   *
   * @param key
   *          The key for this entity
   * @param entity
   *          The entity to store
   */
  public void put(K key, E entity);

  /**
   * Flushes the write buffer, committing any and all entities currently in the
   * buffer.
   */
  public void flush();

  /**
   * This closes and finalizes the batch operation, flushing any remaining
   * entities in the buffer.
   */
  public void close();
}
