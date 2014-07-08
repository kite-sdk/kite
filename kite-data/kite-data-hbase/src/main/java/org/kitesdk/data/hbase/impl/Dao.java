/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.hbase.impl;

import org.kitesdk.data.spi.PartitionKey;
import org.kitesdk.data.PartitionStrategy;

/**
 * Interface for HBase Data Access Objects (DAOs). Supports basic get, put,
 * delete, and scan operations of Java object (entities) over HBase.
 * 
 * @param <E>
 *          The type of entity the DAO should be able to fetch and persist to an
 *          HBase table.
 */
public interface Dao<E> {

  /**
   * Return the entity stored in HBase at the row specified keyed on the
   * PartitionKey key. Returns null if no such entity exists.
   * 
   * @param key
   *          The key of the row to fetch
   * @return The entity of type E, or null if one is not found
   */
  public E get(PartitionKey key);

  /**
   * Put the entity into the HBase table with K key.
   * 
   * @param entity
   *          The entity to store
   * @return True if the put succeeded, False if the put failed due to update
   *         conflict
   */
  public boolean put(E entity);

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
  public long increment(PartitionKey key, String fieldName, long amount);

  /**
   * Deletes the entity in the HBase table at K key.
   * 
   * @param key
   *          The key of the entity to delete.
   */
  public void delete(PartitionKey key);

  /**
   * Deletes the entity in the HBase table. If that entity has a checkConflict
   * field, then the delete will only be performed if the entity has the
   * expected value in the table.
   * 
   * @param entity
   *          The entity, whose checkConflict field may be validated before the
   *          delete is performed.
   * @return True if the put succeeded, False if the put failed due to update
   *         conflict
   */
  public boolean delete(E entity);

  /**
   * Get a scanner to scan the HBase table this DAO reads from. This method
   * opens a scanner starting at the beginning of the table, and will scan to
   * the end.
   * 
   * @return An EntityScanner instance that can be used to iterate through
   *         entities in the table.
   */
  public EntityScanner<E> getScanner();

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
  public EntityScanner<E> getScanner(PartitionKey startKey, PartitionKey stopKey);

  /**
   * Get a scanner to scan the HBase table this DAO reads from. If
   * startInclusive is true, the scanner is opened starting at the first row
   * greater than or equal to startKey. Otherwise, it starts at the first row
   * greater than the startKey. If stopInclusive is true It will stop at the
   * first row it sees greater than the stopKey. Otherwise, it stop at the first
   * row greater than or equal to the stopKey.
   * 
   * If startKey is null, it will start at the first row in the table. If
   * stopKey is null, it will stop at the last row in the table.
   * 
   * @param startKey
   * @param startInclusive
   * @param stopKey
   * @param stopInclusive
   * @return An EntityScanner instance that can be used to iterate through
   *         entities in the table.
   */
  public EntityScanner<E> getScanner(PartitionKey startKey,
      boolean startInclusive, PartitionKey stopKey, boolean stopInclusive);

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
   * Get the entity's PartitonStrategy
   * 
   * @return The PartitionStrategy
   */
  public PartitionStrategy getPartitionStrategy();

  /**
   * Create an EntityBatch with a specified buffer size in bytes
   * 
   * @param writeBufferSize
   *          Write buffer size in bytes
   * @return EntityBatch
   */
  public EntityBatch<E> newBatch(long writeBufferSize);

  /**
   * Create an EntityBatch with the default HBase buffer size.
   * 
   * @return EntityBatch
   */
  public EntityBatch<E> newBatch();
}
