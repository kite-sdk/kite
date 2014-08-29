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

import java.util.Set;

import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;

import org.kitesdk.data.spi.PartitionKey;

/**
 * An interface for mapping HBase Result instances to a StorageKey/Entity pair,
 * and a StorageKey/Entity to an HBase Put instances.
 * 
 * EntityMapper instances should be state-less so they can be reused across
 * multiple Result and Entity instances. They should encapsulate in one place
 * the mapping of business entities to and from HBase.
 * 
 * @param <E>
 *          The entity type
 */
public interface EntityMapper<E> {

  /**
   * Map an Entity of type E to a PartitionKey instance
   *
   * @param entity
   *          The entity which this function will map to a PartitionKey.
   * @return The PartitionKey
   */
  public PartitionKey mapToKey(E entity);

  /**
   * Map an HBase Result instance to an Entity of type E.
   * 
   * @param result
   *          The HBase result instance representing a row from an HBase table.
   * @return The Entity.
   */
  public E mapToEntity(Result result);

  /**
   * Map an Entity of type E to a PutAction instance.
   * 
   * @param entity
   *          The entity which this function will map to a PutAction instance.
   * @return The mapped PutAction.
   */
  public PutAction mapFromEntity(E entity);

  /**
   * Maps a PartitionKey, fieldName and an increment value to an HBase Increment
   * instance that will increment the value in the cell pointed to by fieldName.
   * 
   * @param key
   *          a PartitionKey to use to construct the Increment instance.
   * @param fieldName
   *          The name of the field we are incrementing
   * @param amount
   *          The amount to increment the field by
   * @return An HBase Increment
   */
  public Increment mapToIncrement(PartitionKey key, String fieldName, long amount);

  /**
   * Maps the result of an increment to the new value of the field that was
   * incremented.
   * 
   * @param result
   *          The HBase client Result object that contains the increment result
   * @param fieldName
   *          The name of the field we are getting the increment result for
   * @return The new field value.
   */
  public long mapFromIncrementResult(Result result, String fieldName);

  /**
   * Gets the set of required HBase columns that we would expect to be in the
   * result.
   * 
   * @return The set of required columns.
   */
  public Set<String> getRequiredColumns();

  /**
   * Gets the set of required column families that must exist in the HBase table
   * we would be mapping from.
   * 
   * @return The set of required column families.
   */
  public Set<String> getRequiredColumnFamilies();

  /**
   * Gets the key schema instance for this entity mapper.
   * 
   * @return The KeySchema instance.
   */
  public KeySchema getKeySchema();

  /**
   * Gets the entity schema instance for this entity mapper.
   * 
   * @return The EntitySchema instance.
   */
  public EntitySchema getEntitySchema();

  /**
   * Gets the key serde instance for this entity mapper
   * 
   * @return The key serde for the entity mapper
   */
  public KeySerDe getKeySerDe();

  /**
   * Gets the entity serde instance for this entity mapper
   * 
   * @return The entity serde for the entity mapper
   */
  public EntitySerDe<E> getEntitySerDe();
}
