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
package com.cloudera.data;

import java.io.IOException;

import org.apache.avro.Schema;

/**
 * <p>
 * A logical representation of a set of data entities.
 * </p>
 * <p>
 * Logically, all datasets have two generic properties: a name, and a descriptor
 * that holds information such as the dataset's schema and its partitioning
 * information. Concrete implementations of {@code Dataset} may support
 * additional properties, mandatory or otherwise, as needed. {@code Dataset}s
 * are not normally instantiated directly, but managed by a repository (also
 * implementation-specific).
 * </p>
 * 
 * @see DatasetRepository
 * @see DatasetWriter
 * @see DatasetReader
 * @see PartitionStrategy
 * @see DatasetDescriptor
 * @see Schema
 */
public interface Dataset {

  /**
   * Get the name of a {@code Dataset}. No guarantees about the format of this
   * name are made.
   */
  String getName();

  /**
   * Get the {@link DatasetDescriptor} associated with this dataset.
   */
  DatasetDescriptor getDescriptor();

  /**
   * Get a partition for a {@link PartitionKey}, possibly creating the partition
   * if it doesn't already exist. A {@link PartitionKey} may be obtained using
   * {@link PartitionStrategy#partitionKey(Object...)} or
   * {@link PartitionStrategy#partitionKeyForEntity(Object)}.
   * 
   * @param key
   *          The key used to look up the partition.
   * @param autoCreate
   *          If true, automatically create the partition if doesn't exist,
   *          otherwise, return null.
   */
  Dataset getPartition(PartitionKey key, boolean autoCreate) throws IOException;

  /**
   * <p>
   * Get an appropriate {@link DatasetWriter} implementation based on the
   * underlying {@code Dataset} implementation.
   * </p>
   * <p>
   * Implementations are free to return different types of writers depending on
   * the disposition of the data. For example, a partitioned dataset may use a
   * different writer than that of a non-partitioned dataset. Clients should not
   * make any assumptions about the returned implementations. {@link Dataset}
   * implementations are free to change them at any time.
   * </p>
   * 
   * @throws IOException
   */
  <E> DatasetWriter<E> getWriter() throws IOException;

  /**
   * <p>
   * Get an appropriate {@link DatasetReader} implementation based on the
   * underlying {@code Dataset} implementation.
   * </p>
   * <p>
   * Implementations are free to return different types of readers depending on
   * the disposition of the data. For example, a partitioned dataset may use a
   * different reader than that of a non-partitioned dataset. Clients should not
   * make any assumptions about the returned implementations. {@code Dataset}
   * implementations are free to change them at any time.
   * </p>
   * 
   * @throws IOException
   */
  <E> DatasetReader<E> getReader() throws IOException;

  /**
   * <p>
   * Return partitions, if this dataset is partitioned.
   * </p>
   * <p>
   * Note that, depending on the implementation, the returned iterable may hold
   * system resources until exhausted and/or finalized.
   * </p>
   * 
   * @return an iterable over all partitions of this dataset
   * @throws IOException
   */
  Iterable<Dataset> getPartitions() throws IOException;

}
