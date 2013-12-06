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
 * A {@code View} is a subset of a {@link Dataset}.
 *
 * A {@code View} defines a space of potential storage keys, or a partition
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
   * Returns the underlying {@link org.kitesdk.data.Dataset} that this is a {@code View} of.
   *
   * @return the underlying {@code Dataset}
   */
  Dataset<E> getDataset();

  /**
   * Get an appropriate {@link DatasetReader} implementation based on this
   * {@code View} of the underlying {@code Dataset} implementation.
   *
   * Implementations are free to return different types of readers depending on
   * the disposition of the data. For example, a partitioned dataset may use a
   * different reader than that of a non-partitioned dataset. Clients should not
   * make any assumptions about the returned implementations. Implementations
   * are free to change them at any time.
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
   * make any assumptions about the returned implementations. Implementations
   * are free to change them at any time.
   *
   * @throws DatasetException
   */
  DatasetWriter<E> newWriter();

}
