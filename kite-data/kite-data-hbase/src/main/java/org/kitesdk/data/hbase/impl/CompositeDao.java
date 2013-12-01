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

import java.util.List;

/**
 * The CompositeDao provides an interface for fetching from tables that may have
 * multiple entities stored per row.
 * 
 * The concept is that one entity can be composed from a list of entities
 * that make up the row, and that one entity can be decomposed into multiple sub
 * entities that can be persisted to the row.
 * 
 * @param <E>
 *          The type of the entity this dao returns. This entity will be a
 *          composition of the sub entities.
 * @param <S>
 *          The type of the sub entities.
 */
public interface CompositeDao<E, S> extends Dao<E> {

  /**
   * Compose an entity from the list of sub-entities.
   * 
   * @param entities The list of sub-entities
   * @return The entity instance which contains the composed entity.
   */
  public E compose(List<S> entities);

  /**
   * Decompose an entity into multiple sub entities.
   * 
   * @param entity The entity to decompose
   * @return The list of subentities.
   */
  public List<S> decompose(E entity);
}
