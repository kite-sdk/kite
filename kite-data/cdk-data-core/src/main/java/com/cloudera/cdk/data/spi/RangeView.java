/*
 * Copyright 2013 Cloudera Inc.
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

package com.cloudera.cdk.data.spi;

import com.cloudera.cdk.data.Dataset;
import com.cloudera.cdk.data.View;

interface RangeView<E> extends View<E> {

  /**
   * Deletes the data in this {@link View} or throws an {@code Exception}.
   *
   * Implementations may choose to throw {@link UnsupportedOperationException}
   * for deletes that cannot be easily satisfied by the implementation. For
   * example, in a partitioned {@link Dataset}, the implementation may reject
   * deletes that do not align with partition boundaries. Implementations must
   * document what deletes are supported and under what conditions deletes will
   * be rejected.
   *
   * @return true if any data was deleted; false if the view was already empty
   * @throws UnsupportedOperationException
   *          If the requested delete cannot be completed by the implementation
   * @throws com.cloudera.cdk.data.DatasetIOException
   *          If the requested delete failed because of an IOException
   */
  boolean deleteAll();

  /**
   * Returns whether an entity {@link Object} is in this {@code View}.
   *
   * @param key an entity {@code Object}
   * @return true if {@code key} is in the partition space of this view.
   */
  boolean contains(E key);

  /**
   * Returns whether a {@link Marker} is in this {@code View}
   *
   * @param marker a {@code Marker}
   * @return true if {@code marker} is in the partition space of this view.
   */
  boolean contains(Marker marker);

  /**
   * Returns an Iterable of non-overlapping {@link View} objects that partition
   * the underlying {@link com.cloudera.cdk.data.Dataset} and cover this {@code View}.
   *
   * The returned {@code View} objects are implementation-specific, but should
   * represent reasonable partitions of the underlying {@code Dataset} based on
   * its layout.
   *
   * The data contained by the union of each {@code View} in the Iterable must
   * be a super-set of this {@code View}.
   *
   * Note that partitions are actual partitions under which data is stored.
   * Implementations are encouraged to omit any {@code View} that is empty.
   *
   * This method is intended to be used by classes like InputFormat, which need
   * to enumerate the underlying partitions to create InputSplits.
   *
   * @return
   *      An Iterable of the {@code View} that cover this {@code View}.
   * @throws IllegalStateException
   *      If the underlying {@code Dataset} is not partitioned.
   */
  Iterable<View<E>> getCoveringPartitions();

  /**
   * Creates a sub-{@code View}, from the {@code start} {@link Marker} to the
   * end of this {@code View}.
   *
   * The returned View is inclusive: the partition space contained by the start
   * Marker is included.
   *
   * {@code start} must be contained by this {@code View}, as determined by
   * {@link #contains(Marker)}.
   *
   * @param start a starting {@code Marker}
   * @return a {@code View} for the range
   *
   * @throws IllegalArgumentException If {@code start} is null or not in this
   *                                  {@code View}.
   */
  RangeView<E> from(Marker start);

  /**
   * Creates a sub-{@code View}, from after the {@code start} {@link Marker} to
   * the end of this {@code View}.
   *
   * The returned View is exclusive: the partition space contained by the start
   * Marker is not included.
   *
   * {@code start} must be contained by this {@code View}, as determined by
   * {@link #contains(Marker)}.
   *
   * @param start a starting {@code Marker}
   * @return a {@code View} for the range
   *
   * @throws IllegalArgumentException If {@code start} is null or not in this
   *                                  {@code View}.
   */
  RangeView<E> fromAfter(Marker start);

  /**
   * Creates a sub-{@code View}, from the start of this {@code View} to the
   * {@code end} {@link Marker}.
   *
   * The returned View is inclusive: the space contained by the end Marker is
   * included.
   *
   * {@code end} must be contained by this {@code View}, as determined by
   * {@link #contains(Marker)}.
   *
   * @param end an ending {@code Marker}
   * @return a {@code View} for the range
   *
   * @throws IllegalArgumentException If {@code end} is null or not in this
   *                                  {@code View}.
   */
  RangeView<E> to(Marker end);

  /**
   * Creates a sub-{@code View}, from the start of this {@code View} to before
   * the {@code end} {@link Marker}.
   *
   * The returned View is exclusive: the space contained by the end Marker is
   * not included.
   *
   * {@code end} must be contained by this {@code View}, as determined by
   * {@link #contains(Marker)}.
   *
   * @param end an ending {@code Marker}
   * @return a {@code View} for the range
   *
   * @throws IllegalArgumentException If {@code end} is null or not in this
   *                                  {@code View}.
   */
  RangeView<E> toBefore(Marker end);

  /**
   * Creates a sub-{@code View} from the partition space contained by a partial
   * {@link Marker}.
   *
   * The returned View will contain all partitions that match the values from
   * {@code Marker} for a {@link com.cloudera.cdk.data.PartitionStrategy}. For example, consider the
   * following {@code Marker} and {@code PartitionStrategy}:
   * <pre>
   * partial = new Marker.Builder("time", 1380841757896).get();
   * mmdd = new PartitionStrategy.Builder().month("time").day("time").get();
   * </pre>
   *
   * The {@code Marker} contains partitions only given a concrete
   * {@code PartitionStrategy}, which determines the values that will be
   * matched. Above, the {@code Marker} contains all October 3rd partitions.
   * But, if the {@code PartitionStrategy} contained a year field, it would
   * contain only October 3rd for 2013.
   *
   * To limit how specific a {@code Marker} is, it can be constructed using
   * concrete values rather than source values. This example limits the partial
   * to just October, even though the partition strategy includes the day:
   * <pre>
   * partial = new Marker.Builder("month", OCTOBER).get();
   * mmdd = new PartitionStrategy.Builder().month("time").day("time").get();
   * </pre>
   *
   * {@code partial} must be contained by this {@code View}, as determined by
   * {@link #contains(Marker)}.
   *
   * @param partial a partial {@code Marker}
   * @return a {@code View} of the partition space under {@code partial}.
   *
   * @throws IllegalArgumentException If {@code partial} is null or not in this
   *                                  {@code View}.
   */
  RangeView<E> of(Marker partial);
}
