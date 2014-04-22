/*
 * Copyright 2013 Cloudera.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data;

import javax.annotation.concurrent.Immutable;

/**
 * <p>
 * A {@link Dataset} that supports random access operations on entities.
 * </p>
 * <p>
 * Implementations of {@link RandomAccessDataset} are immutable.
 * </p>
 *
 * @param <E> The type of entities stored in this {@code RandomAccessDataset}.
 *
 * @see Dataset
 * @since 0.9.0
 */
@Immutable
public interface RandomAccessDataset<E> extends Dataset<E> {
  
  /**
   * Return the entity stored in the dataset at the row specified with {@link
   * Key} <code>key</code>. Return null if no such entity exists.
   *
   * @param key
   *          The key of the entity to get
   * @return The entity of type E, or null if one is not found
   */
  public E get(Key key);

  /**
   * Put the entity into the dataset.
   *
   * @param entity
   *          The entity to store
   * @return True if the put succeeded, false if the put failed due to an update
   *         conflict
   */
  public boolean put(E entity);

  /**
   * Increment a field named <code>fieldName</code> on the entity by the
   * specified amount.
   *
   * @param key
   *          The key of the entity to increment
   * @param fieldName
   *          The name of the field on the entity to increment. If the fieldName
   *          doesn't exist, an exception is thrown.
   * @param amount
   *          The amount to increment the field by
   * @return The new field amount.
   */
  public long increment(Key key, String fieldName, long amount);

  /**
   * Deletes the entity in the dataset with {@link Key} <code>key</code>.
   *
   * @param key
   *          The key of the entity to delete.
   */
  public void delete(Key key);

  /**
   * Deletes the entity passed to this method in the dataset.
   * If that entity has a checkConflict field, then the delete is performed only 
   * if the entity in the dataset has the same value as the one in the
   * passed <code>entity</code> object.
   *.
   * @param entity
   *          The entity, whose checkConflict field can be validated before the
   *          delete is performed
   * @return True if the delete succeeded, false if the delete failed due to an 
   *         update conflict
   */
  public boolean delete(E entity);
}
