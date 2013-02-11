package com.cloudera.data;

import java.io.IOException;

import org.apache.avro.Schema;

/**
 * <p>
 * A logical repository (storage system) of {@link Dataset}s.
 * </p>
 * <p>
 * Implementations of {@code DatasetRepository} are storage systems that contain
 * zero or more {@link Dataset}s. A repository acts as a factory, as well as a
 * registry, of datasets. Users can {@link #create(String, Schema)} a new
 * {@link Dataset} with a name and schema, or retrieve a handle to an existing
 * dataset, by name, by way of the {@link #get(String)} method. While not
 * expressly forbidden, most repositories are expected to support only a single
 * concrete {@link Dataset} implementation.
 * </p>
 * <p>
 * No guarantees are made as to the durability, reliability, or availability of
 * the underlying storage. That is, a {@code DatasetRepository} could be on
 * disk, in memory, or some combination.
 * </p>
 * 
 * @param <DS>
 *          The concrete implementation of {@link Dataset} on which a repository
 *          operates.
 */
public interface DatasetRepository<DS extends Dataset> {

  /**
   * Get the latest version of a named {@link Dataset}. If no dataset with the
   * provided {@code name} exists, an {@link IOException} is thrown.
   * 
   * @param name
   *          The name of the dataset.
   * @throws IOException
   */
  DS get(String name) throws IOException;

  /**
   * Create a {@link Dataset} with the supplied {@code schema}. Depending on the
   * underlying dataset storage, some schemas types or configurations may not be
   * supported. If an illegal schema is supplied, an exception will be thrown by
   * the implementing class. It is illegal to create a more than one dataset
   * with a given name. If a duplicate name is provided, an exception is thrown.
   * 
   * @param name
   *          The fully qualified dataset name
   * @param schema
   *          A schema that describes entities of this dataset
   * @return The newly created dataset
   * @throws IOException
   */
  DS create(String name, Schema schema) throws IOException;

}
