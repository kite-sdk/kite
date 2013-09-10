// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

/**
 * Interface for HBase Common DAOs. Supports basic get, put, delete, and scan
 * operations over HBase.
 *
 * Almost all access and modifier functions take a row key. This key is
 * represented by the Key<T> type. The type parameter <T> on the key type is the
 * concrete java type that underlies the Key. A key can be constructed from this
 * concrete type. All access and modifier methods on the DAO also support the
 * ability to pass in this underlying type directly.
 *
 * @param <K>
 *          The underlying key type.
 * @param <E>
 *          The type of entity the DAO should return.
 */
public interface Dao<K, E> {

  /**
   * Return the entity stored in HBase at the row specified with Key key. Return
   * null if no such entity exists.
   *
   * @param key
   *          The key entity to get
   * @return The entity of type T, or null if one is not found
   */
  public E get(K key);

  /**
   * Put the entity into the HBase table with K key.
   *
   * @param key
   * @param entity
   *          The entity to store
   * @return True if the put succeeded, False if the put failed due to update
   *         conflict
   */
  public boolean put(K key, E entity);

  /**
   * Increment a field named fieldName on the entity by value.
   * 
   * @param key
   *          The key of the entity to increment
   * @param fieldName
   *          The name of the field on the entity to increment. If the fieldName
   *          doesn't exist, an exception will be thrown.
   * @param amount
   *          The amount to increment the field by
   * @return The new field amount.
   */
  public long increment(K key, String fieldName, long amount);

  /**
   * Deletes the entity in the HBase table at K key.
   *
   * @param key
   *          The key of the entity to delete.
   */
  public void delete(K key);

  /**
   * Deletes the entity in the HBase table at K key. If that entity has a
   * checkConflict field, then the delete will only be performed if the entity
   * has the expected value in the table.
   *
   * @param key
   *          The key of the entity to delete.
   * @param entity
   *          The entity, whose checkConflict field may be validated before the
   *          delete is performed.
   * @return True if the put succeeded, False if the put failed due to update
   *         conflict
   */
  public boolean delete(K key, E entity);

  /**
   * Get a scanner to scan the HBase table this DAO reads from. This method
   * opens a scanner starting at the beginning of the table, and will scan to
   * the end.
   *
   * @return An EntityScanner instance that can be used to iterate through
   *         entities in the table.
   */
  public EntityScanner<K, E> getScanner();

  /**
   * Get a scanner to scan the HBase table this DAO reads from. The scanner is
   * opened starting at the first row greater than or equal to startKey. It will
   * stop at the first row it sees greater than or equal to stopKey.
   *
   * If startKey is null, it will start at the first row in the table. If
   * stopKey is null, it will stop at the last row in the table.
   *
   * @param startKey
   * @param stopKey
   * @return An EntityScanner instance that can be used to iterate through
   *         entities in the table.
   */
  public EntityScanner<K, E> getScanner(PartialKey<K> startKey, PartialKey<K> stopKey);

  /**
   * Get a scanner to scan the HBase table this DAO reads from. The scanner is
   * opened starting at the first row greater than or equal to startKey. It will
   * stop at the first row it sees greater than or equal to stopKey.
   *
   * If startKey is null, it will start at the first row in the table. If
   * stopKey is null, it will stop at the last row in the table.
   *
   * @param startKey
   * @param stopKey
   * @return An EntityScanner instance that can be used to iterate through
   *         entities in the table.
   */
  public EntityScanner<K, E> getScanner(K startKey, K stopKey);

  /**
   * Get a scanner to scan the HBase table this DAO reads from. The scanner is
   * opened starting at the first row greater than or equal to startKey. It will
   * stop at the first row it sees greater than or equal to stopKey. Scan will
   * be modified using given scan modifier.
   *
   * If startKey is null, it will start at the first row in the table. If
   * stopKey is null, it will stop at the last row in the table.
   *
   * @param startKey
   *          The start key of the scan
   * @param stopKey
   *          The stop key of the scan. The scanner will scan up to, but not
   *          including the row of the stopKey.
   * @param scanModifier
   *          A scan modifier which can modify the HBase Scan instance before
   *          sending it to HBase
   * @return An EntityScanner instance that can be used to iterate through
   *         entities in the table.
   */
  public EntityScanner<K, E> getScanner(PartialKey<K> startKey, PartialKey<K> stopKey,
      ScanModifier scanModifier);

  /**
   * Get a scanner to scan the HBase table this dao reads from. The scanner is
   * opened starting at the first row greater than or equal to startKey. It will
   * stop at the first row it sees greater than or equal to stopKey.Scan will be
   * modified using given scan modifier.
   *
   * If startKey is null, it will start at the first row in the table. If
   * stopKey is null, it will stop at the last row in the table.
   *
   * @param startKey
   *          The start key of the scan
   * @param stopKey
   *          The stop key of the scan. The scanner will scan up to, but not
   *          including the row of the stopKey.
   * @param scanModifier
   *          A scan modifier which can modify the HBase Scan instance before
   *          sending it to HBase
   * @return An EntityScanner instance that can be used to iterate through
   *         entities in the table.
   */
  public EntityScanner<K, E> getScanner(K startKey, K stopKey,
      ScanModifier scanModifier);

  /**
   * Gets the ScannerBuilder for this DAO.
   * 
   * @return ScannerBuilder
   */
  public EntityScannerBuilder<K, E> getScannerBuilder();

  /**
   * Gets the key schema instance for this DAO.
   *
   * @return The HBaseCommonKeySchema instance.
   */
  public KeySchema getKeySchema();

  /**
   * Gets the entity schema instance for this DAO.
   *
   * @return The HBaseCommonEntitySchema instance.
   */
  public EntitySchema getEntitySchema();

  /**
   * Create an EntityBatch with a specified buffer size in bytes
   *
   * @param writeBufferSize
   *          Write buffer size in bytes
   * @return EntityBatch
   */
  public EntityBatch<K, E> newBatch(long writeBufferSize);

  /**
   * Create an EntityBatch with the default HBase buffer size.
   *
   * @return EntityBatch
   */
  public EntityBatch<K, E> newBatch();
}
