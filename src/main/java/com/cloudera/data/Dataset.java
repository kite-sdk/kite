package com.cloudera.data;

import java.io.IOException;

import org.apache.avro.Schema;

/**
 * A logical representation of a set of data entities.
 * 
 * Logically, all datasets have two generic properties: a name, and a schema
 * that describes an entity of that dataset. Concrete implementations of
 * {@code Dataset} may support additional properties, mandatory or otherwise, as
 * needed. {@code Dataset}s are not normally instantiated directly, but managed
 * by a repository (also implementation-specific).
 * 
 * @see DatasetRepository
 * @see DatasetWriter
 * @see DatasetReader
 * @see PartitionStrategy
 * @see Schema
 */
public interface Dataset {

  /**
   * Get the name of a {@code Dataset}. No guarantees about the format of this
   * name are made.
   */
  String getName();

  /**
   * Get the {@code Dataset}'s associated {@link Schema}. Depending on the
   * underlying storage system, this schema may be simple (i.e. records made up
   * of only scalar types) or complex (i.e. containing other records, lists, and
   * so on). Validation of the supported schemas is performed by the managing
   * repository, not the {@code Dataset} itself.
   * 
   * @return the dataset schema
   */
  Schema getSchema();

  /**
   * Get the {@link PartitionStrategy}, if this dataset is partitioned. Calling
   * this method on a non-partitioned dataset is an error. Instead, use the
   * {@link #isPartitioned()} method prior to invocation.
   */
  PartitionStrategy getPartitionStrategy();

  /**
   * Returns true if the dataset is partitioned (that is, has an associated
   * {@link PartitionStrategy}, false otherwise.
   */
  boolean isPartitioned();

  /**
   * Get a partition for an entity, possibly creating the partition if it doesn't
   * already exist.
   * 
   * @param entity
   *          The entity being used to find the partition.
   * @param autoCreate
   *          If true, automatically create the partition if doesn't exist,
   *          otherwise, return null.
   */
  <E> Dataset getPartition(E entity, boolean autoCreate) throws IOException;

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
