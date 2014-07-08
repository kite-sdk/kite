/*
 * Copyright 2014 Cloudera Inc.
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
package org.kitesdk.data.spi;

import org.kitesdk.data.Dataset;

@SuppressWarnings("deprecation")
public interface PartitionedDataset<E> extends Dataset<E> {

  /**
   * Get a partition for a {@link PartitionKey}, optionally creating the
   * partition if it doesn't already exist. You can obtain the
   * {@link PartitionKey} using
   * {@link org.kitesdk.data.PartitionStrategy#partitionKey(Object...)} or
   * {@link org.kitesdk.data.PartitionStrategy#partitionKeyForEntity(Object)}.
   *
   * @param key        The key used to look up the partition.
   * @param autoCreate If true, automatically creates the partition if it
   * doesn't exist.
   * @throws org.kitesdk.data.DatasetException
   */
  PartitionedDataset<E> getPartition(PartitionKey key, boolean autoCreate);

  /**
   * Drop a partition for a {@link PartitionKey}. Dropping a partition that
   * doesn't exist results in a {@link org.kitesdk.data.DatasetException} being thrown.
   *
   * @param key The key used to look up the partition.
   * @throws org.kitesdk.data.DatasetException
   * @since 0.2.0
   */
  void dropPartition(PartitionKey key);

  /**
   * <p>
   * Return partitions.
   * </p>
   * <p>
   * Note that, depending on the implementation, the returned iterable can hold
   * system resources until exhausted and/or finalized.
   * </p>
   *
   * @return an iterable over all partitions of this dataset
   * @throws org.kitesdk.data.DatasetException
   */
  Iterable<PartitionedDataset<E>> getPartitions();
}
