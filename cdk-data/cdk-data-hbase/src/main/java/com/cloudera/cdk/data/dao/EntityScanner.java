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

/**
 * A Scanner interface that represents an Iterable that allows us to iterate
 * over entities in an HBase table, returning them as KeyEntity instances.
 * 
 * @param <K>
 *          The underlying key record type
 * @param <E>
 *          The type of the entity to return
 */
public interface EntityScanner<K, E> extends Iterable<KeyEntity<K, E>>, Closeable {

  /**
   * Opens the scanner over the table, with scan parameters.
   */
  public void open();

  /**
   * Closes the entity scanner, and cleans up any underlying resources.
   */
  @Override
  public void close();
}
