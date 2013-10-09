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
package com.cloudera.cdk.data.dao;

import java.io.Closeable;
import java.io.Flushable;

public interface EntityBatch<E> extends Flushable, Closeable {

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
  public void put(E entity);

  /**
   * Flushes the write buffer, committing any and all entities currently in the
   * buffer.
   */
  @Override
  public void flush();

  /**
   * This closes and finalizes the batch operation, flushing any remaining
   * entities in the buffer.
   */
  @Override
  public void close();
}
