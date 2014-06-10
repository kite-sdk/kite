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
import org.kitesdk.data.spi.ReadySignalable;

/**
 * A {@code View} is a subset of a {@link Dataset}.
 *
 * A {@code View} defines a space of potential storage keys or a partition
 * space. Views can be created from ranges, partial keys, or the union of other
 * views.
 *
 * @param <E>
 *      The type of entities stored in the {@code Dataset} underlying this
 *      {@code View}.
 * @since 0.9.0
 */
@Immutable
public interface View<E> {

  /**
   * Returns the underlying {@link org.kitesdk.data.Dataset} for the
   * {@code View}.
   *
   * @return the underlying {@code Dataset}
   */
  Dataset<E> getDataset();

  /**
   * Get an appropriate {@link DatasetReader} implementation based on this
   * {@code View} of the underlying {@code Dataset} implementation.
   *
   * Implementations are free to return different types of readers, depending on
   * the disposition of the data. For example, a partitioned dataset can use a
   * different reader than that of a non-partitioned dataset. Clients should not
   * make any assumptions about the returned implementations: implementations
   * are free to change their internal structure at any time.
   *
   * @throws DatasetException
   */
  DatasetReader<E> newReader();

  /**
   * Get an appropriate {@link DatasetWriter} implementation based on this
   * {@code View} of the underlying {@code Dataset} implementation.
   *
   * Implementations are free to return different types of writers depending on
   * the disposition of the data. For example, a partitioned dataset may use a
   * different writer than that of a non-partitioned dataset. Clients should not
   * make any assumptions about the returned implementations: implementations
   * are free to change their internal structure at any time.
   *
   * @throws DatasetException
   */
  DatasetWriter<E> newWriter();

  /**
   * Returns whether an entity {@link Object} would be included in this {@code View} if
   * it were present in the {@code Dataset}.
   *
   * @param entity an entity {@code Object}
   * @return true if {@code entity} is in the partition space of this view.
   * @since 0.11.0
   */
  boolean includes(E entity);

  /**
   * Deletes the entities included in this {@link View} or throws an
   * {@link UnsupportedOperationException}.
   *
   * Implementations are allowed to throw {@code UnsupportedOperationException}
   * if the {@code View} could require additional work to delete. For example,
   * if some but not all of the data in an underlying data file must be removed,
   * then the implementation is allowed to reject the deletion rather than
   * copy the remaining records to a new file. Implementations must  document
   * what deletes are supported and under what conditions deletes are rejected.
   * 
   * @return true if any data was deleted, false if the View was already empty
   * @throws UnsupportedOperationException
   *          if the requested delete cannot be completed by the implementation
   * @throws DatasetIOException
   *          if the requested delete failed because of an IOException
   * @since 0.12.0
   */
  public boolean deleteAll();
  
  /**
   * Indicates whether the View has been signaled as being ready for consumption
   * by downstream processing. Some views do not support being signaled ready
   * and in such cases, this method will throw
   * {@link NotReadySignalableException}.
   * 
   * @return true if the view supports signaling and has been signaled
   * @throws NotReadySignalableException
   *           if the view does not support ready signaling
   */
  public boolean isReady();
}
