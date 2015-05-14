/*
 * Copyright 2015 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data;

import java.net.URI;
import javax.annotation.concurrent.Immutable;

/**
 * A {@code PartitionView} is a subset of a {@link Dataset} that corresponds to
 * a single partition, with a location.
 *
 * @param <E>
 *      The type of entities stored in the {@code Dataset} underlying this
 *      {@code View}.
 * @since 1.1.0
 */
@Immutable
public interface PartitionView<E> extends View<E> {
  /**
   * Returns the location of this partition as a {@link URI}.
   *
   * @return a {@code URI} for the location of this partition.
   * @since 1.1.0
   */
  public URI getLocation();

  /**
   * Deletes the entities included in this {@link View}.
   * <p>
   * Unlike {@link View#deleteAll()}, implementations are not allowed to throw
   * {@link UnsupportedOperationException} and must support this method in all
   * {@code PartitionView} implementations.
   *
   * @return true if any data was deleted, false if the View was already empty
   * @throws DatasetIOException
   *          if the requested delete failed because of an IOException
   * @since 1.1.0
   */
  @Override
  boolean deleteAll();

  /**
   * Get an appropriate {@link DatasetWriter} implementation based on the
   * constraints for this  {@code View} of the underlying {@code Dataset}.
   * <p>
   * Although the {@code view} is limited to a particular location in the
   * dataset when reading, writers returned by this method will not necessarily
   * write to that location. Instead, writers are subject to the logical
   * constraints of this {@code PartitionView}.
   * <p>
   * Implementations are free to return different types of writers depending on
   * the disposition of the data. For example, a partitioned dataset may use a
   * different writer than that of a non-partitioned dataset. Clients should not
   * make any assumptions about the returned implementations: implementations
   * are free to change their internal structure at any time.
   *
   * @throws DatasetException
   */
  @Override
  DatasetWriter<E> newWriter();
}
