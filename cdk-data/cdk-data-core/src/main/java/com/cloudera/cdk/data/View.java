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
package com.cloudera.cdk.data;

import javax.annotation.concurrent.Immutable;

/**
 * A {@code View} is a subset of a {@link Dataset}.
 *
 * A {@code View} defines a space of potential PartitionKeys, or a partition
 * space. Views can be created from ranges, partial keys, or the union of other
 * views.
 *
 * @param <E>
 *      The type of entities stored in the {@code Dataset} underlying this
 *      {@code View}.
 */
@Immutable
public interface View<E> {

  /**
   * Returns the underlying {@link Dataset} that this is a {@code View} of.
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

  /**
   * Get an appropriate {@link DatasetAccessor} implementation based on this
   * {@code View} of the underlying {@code Dataset} implementation.
   *
   * Implementations are free to return different types of accessors depending
   * on the disposition of the data. For example, a partitioned dataset may use
   * a different accessor than that of a non-partitioned dataset. Clients should
   * not make any assumptions about the returned implementations.
   * Implementations are free to change them at any time.
   *
   * @throws DatasetException
   */
  DatasetAccessor<E> newAccessor();

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
   * the underlying {@link Dataset} and cover this {@code View}.
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

  // TODO: should the View factory methods support entities also?

  /**
   * Creates a sub-{@code View} from the given range, represented by
   * {@code start} and {@code end} {@link Marker} objects.
   *
   * The returned View is inclusive: the space contained by both Markers is
   * included.
   *
   * Both {@code start} and {@code end} must be contained by this {@code View},
   * as determined by {@link #contains(com.cloudera.cdk.data.Marker)}.
   *
   * If either Marker is null, the start or end of this {@code View} will be
   * used.
   *
   * @param start a starting {@code Marker}
   * @param end an ending {@code Marker}
   * @return a {@code View} for the range
   *
   * @throws IllegalArgumentException If either Marker is not in this
   *                                  {@code View}.
   */
  //View<E> range(Marker start, Marker end);

  /**
   * Creates a sub-{@code View}, from the {@code start} {@link Marker} to the
   * end of this {@code View}.
   *
   * The returned View is inclusive: the partition space contained by the start
   * Marker is included.
   *
   * {@code start} must be contained by this {@code View}, as determined by
   * {@link #contains(com.cloudera.cdk.data.Marker)}.
   *
   * @param start a starting {@code Marker}
   * @return a {@code View} for the range
   *
   * @throws IllegalArgumentException If {@code start} is null or not in this
   *                                  {@code View}.
   */
  View<E> from(Marker start);

  /**
   * Creates a sub-{@code View}, from after the {@code start} {@link Marker} to
   * the end of this {@code View}.
   *
   * The returned View is exclusive: the partition space contained by the start
   * Marker is not included.
   *
   * {@code start} must be contained by this {@code View}, as determined by
   * {@link #contains(com.cloudera.cdk.data.Marker)}.
   *
   * @param start a starting {@code Marker}
   * @return a {@code View} for the range
   *
   * @throws IllegalArgumentException If {@code start} is null or not in this
   *                                  {@code View}.
   */
  View<E> fromAfter(Marker start);

  /**
   * Creates a sub-{@code View}, from the start of this {@code View} to the
   * {@code end} {@link Marker}.
   *
   * The returned View is inclusive: the space contained by the end Marker is
   * included.
   *
   * {@code end} must be contained by this {@code View}, as determined by
   * {@link #contains(com.cloudera.cdk.data.Marker)}.
   *
   * @param end an ending {@code Marker}
   * @return a {@code View} for the range
   *
   * @throws IllegalArgumentException If {@code end} is null or not in this
   *                                  {@code View}.
   */
  View<E> to(Marker end);

  /**
   * Creates a sub-{@code View}, from the start of this {@code View} to before
   * the {@code end} {@link Marker}.
   *
   * The returned View is exclusive: the space contained by the end Marker is
   * not included.
   *
   * {@code end} must be contained by this {@code View}, as determined by
   * {@link #contains(com.cloudera.cdk.data.Marker)}.
   *
   * @param end an ending {@code Marker}
   * @return a {@code View} for the range
   *
   * @throws IllegalArgumentException If {@code end} is null or not in this
   *                                  {@code View}.
   */
  View<E> toBefore(Marker end);

  /**
   * Creates a sub-{@code View} from the partition space contained by a partial
   * {@link Marker}.
   *
   * The returned View will contain all partitions that match the values from
   * {@code Marker} for a {@link PartitionStrategy}. For example, consider the
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
   * {@link #contains(com.cloudera.cdk.data.Marker)}.
   *
   * @param partial a partial {@code Marker}
   * @return a {@code View} of the partition space under {@code partial}.
   *
   * @throws IllegalArgumentException If {@code partial} is null or not in this
   *                                  {@code View}.
   */
  View<E> in(Marker partial);
}
