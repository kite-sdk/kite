package com.cloudera.cdk.data.hbase;

/**
 * A class that encapsulates a Key and an Entity which is returned from the
 * EntityMapper mapToEntity function.
 *
 * @param <K>
 *          The underlying key record type
 * @param <E>
 *          The entity type
 */
public class KeyEntity<K, E> {
  private final K key;
  private final E entity;

  public KeyEntity(K key, E entity) {
    this.key = key;
    this.entity = entity;
  }

  public K getKey() {
    return key;
  }

  public E getEntity() {
    return entity;
  }
}
