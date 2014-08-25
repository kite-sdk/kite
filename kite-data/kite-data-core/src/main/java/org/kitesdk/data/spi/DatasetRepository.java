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
package org.kitesdk.data.spi;

import java.net.URI;
import java.util.Collection;
import javax.annotation.concurrent.Immutable;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.Datasets;

/**
 * <p>
 * A logical repository (storage system) of {@link Dataset}s.
 * </p>
 * <p>
 * Implementations of {@code DatasetRepository} are storage systems that contain
 * zero or more {@link Dataset}s. A repository acts as a factory, as well as a
 * registry, of datasets. You can {@link #create(String, DatasetDescriptor)} a
 * new {@link Dataset} with a name and schema, or retrieve a handle to an
 * existing dataset, by name, by way of the {@link #load(String)} method. While
 * not expressly forbidden, most repositories are expected to support only a
 * single concrete {@link Dataset} implementation.
 * </p>
 * <p>
 * No guarantees are made as to the durability, reliability, or availability of
 * the underlying storage. That is, a {@code DatasetRepository} could be on
 * disk, in memory, or some combination. See the implementation class for
 * details about the guarantees it provides.
 * </p>
 * <p>
 * Implementations of {@link DatasetRepository} are immutable.
 * </p>
 *
 * @see Dataset
 * @see DatasetDescriptor
 */
@Immutable
public interface DatasetRepository {

  /**
   * Get the latest version of a named {@link Dataset}. If no dataset with the
   * provided {@code name} exists, a {@link DatasetNotFoundException} is thrown.
   *
   * @param name The name of the dataset.
   * @throws DatasetNotFoundException if there is no data set named {@code name}
   *
   * @since 0.7.0
   */
  <E> Dataset<E> load(String name);

  /**
   * Get the latest version of a named {@link Dataset}. If no dataset with the
   * provided {@code name} exists, a {@link DatasetNotFoundException} is thrown.
   *
   * @param name The name of the dataset.
   * @param type the Java type of entities in the dataset
   * @throws DatasetNotFoundException if there is no data set named {@code name}
   *
   * @since 0.15.0
   */
  <E> Dataset<E> load(String name, Class<E> type);

  /**
   * Create a {@link Dataset} with the supplied {@code descriptor}. Depending on
   * the underlying dataset storage, some schema types or configurations might
   * not be supported. If you supply an illegal schema, the implementing class
   * throws an exception. It is illegal to create more than one dataset with the
   * same name. If you provide a duplicate name, the implementing class throws
   * an exception.
   *
   * @param name        The fully qualified dataset name
   * @param descriptor  A descriptor that describes the schema and other
   *                    properties of the dataset
   * @return The newly created dataset
   * @throws IllegalArgumentException   if {@code name} or {@code descriptor}
   *                                    is {@code null}
   * @throws DatasetExistsException     if a {@code Dataset} named {@code name}
   *                                    already exists.
   * @throws ConcurrentSchemaModificationException
   *                                    if the {@code Dataset}
   *                                    schema is updated
   *                                    concurrently.
   * @throws IncompatibleSchemaException
   *                                    if the schema is not
   *                                    compatible with existing
   *                                    datasets with shared
   *                                    storage (for example, in the
   *                                    same HBase table).
   * @throws DatasetRepositoryException
   */
  <E> Dataset<E> create(String name, DatasetDescriptor descriptor);

  /**
   * Create a {@link Dataset} with the supplied {@code descriptor}. Depending on
   * the underlying dataset storage, some schema types or configurations might
   * not be supported. If you supply an illegal schema, the implementing class
   * throws an exception. It is illegal to create more than one dataset with the
   * same name. If you provide a duplicate name, the implementing class throws
   * an exception.
   *
   * @param name        The fully qualified dataset name
   * @param descriptor  A descriptor that describes the schema and other
   *                    properties of the dataset
   * @param type        the Java type of entities in the dataset
   * @return The newly created dataset
   * @throws IllegalArgumentException   if {@code name} or {@code descriptor}
   *                                    is {@code null}
   * @throws DatasetExistsException     if a {@code Dataset} named {@code name}
   *                                    already exists.
   * @throws ConcurrentSchemaModificationException
   *                                    if the {@code Dataset}
   *                                    schema is updated
   *                                    concurrently.
   * @throws IncompatibleSchemaException
   *                                    if the schema is not
   *                                    compatible with existing
   *                                    datasets with shared
   *                                    storage (for example, in the
   *                                    same HBase table).
   *
   * @since 0.15.0
   */
  <E> Dataset<E> create(String name, DatasetDescriptor descriptor, Class<E> type);

  /**
   * Update an existing {@link Dataset} to reflect the supplied
   * {@code descriptor}. The common case is updating a dataset schema. Depending
   * on the underlying dataset storage, some updates might not be supported,
   * such as a change in format or partition strategy. Any attempt to make an
   * unsupported or incompatible update results in an exception being thrown 
   * and no changes made to the dataset.
   *
   * @param name       The fully qualified dataset name
   * @param descriptor A descriptor that describes the schema and other
   *                   properties of the dataset
   * @return The updated dataset
   * @throws IllegalArgumentException      if {@code name} is null
   * @throws DatasetNotFoundException      if there is no data set named
   *                                       {@code name}
   * @throws UnsupportedOperationException if descriptor updates are not
   *                                       supported by the implementation
   * @throws ConcurrentSchemaModificationException
   *                                       if the {@code Dataset}
   *                                       schema is updated
   *                                       concurrently
   * @throws IncompatibleSchemaException
   *                                    if the schema is not
   *                                    compatible with
   *                                    previous schemas,
   *                                    or with existing
   *                                    datasets with shared
   *                                    storage (for example, in the
   *                                    same HBase table).
   *
   * @since 0.3.0
   */
  <E> Dataset<E> update(String name, DatasetDescriptor descriptor);

  /**
   * Update an existing {@link Dataset} to reflect the supplied
   * {@code descriptor}. The common case is updating a dataset schema. Depending
   * on the underlying dataset storage, some updates might not be supported,
   * such as a change in format or partition strategy. Any attempt to make an
   * unsupported or incompatible update results in an exception being thrown 
   * and no changes made to the dataset.
   *
   * @param name       The fully qualified dataset name
   * @param descriptor A descriptor that describes the schema and other
   *                   properties of the dataset
   * @param type        the Java type of entities in the dataset
   * @return The updated dataset
   * @throws IllegalArgumentException      if {@code name} is null
   * @throws DatasetNotFoundException      if there is no data set named
   *                                       {@code name}
   * @throws UnsupportedOperationException if descriptor updates are not
   *                                       supported by the implementation
   * @throws ConcurrentSchemaModificationException
   *                                       if the {@code Dataset}
   *                                       schema is updated
   *                                       concurrently
   * @throws IncompatibleSchemaException
   *                                    if the schema is not
   *                                    compatible with
   *                                    previous schemas,
   *                                    or with existing
   *                                    datasets with shared
   *                                    storage (for example, in the
   *                                    same HBase table).
   *
   * @since 0.15.0
   */
  <E> Dataset<E> update(String name, DatasetDescriptor descriptor, Class<E> type);

  /**
   * Delete data for the {@link Dataset} named {@code name} and remove its
   * {@link DatasetDescriptor} from the underlying metadata provider.
   *
   * After this method is called, there is no {@code Dataset} with the given
   * {@code name}, unless an exception is thrown. If either data or metadata
   * are removed, this method returns {@code true}. If there is no
   * {@code Dataset} corresponding to the given {@code name}, this
   * method makes no changes and returns {@code false}.
   *
   * @param name The name of the dataset to delete.
   * @return {@code true} if any data or metadata is removed,
   *         {@code false} if no action is taken.
   * @throws IllegalArgumentException if {@code name} is null
   * @throws ConcurrentSchemaModificationException
   *                                  if the {@code Dataset}
   *                                  schema is updated concurrently.
   *
   * @since 0.7.0
   */
  boolean delete(String name);

  /**
   * Checks if there is a {@link Dataset} in this repository named {@code name}.
   *
   * @param name a {@code Dataset} name to check the existence of
   * @return true if a Dataset named {@code name} exists, false otherwise
   * @throws IllegalArgumentException if {@code name} is null
   *
   * @since 0.7.0
   */
  boolean exists(String name);

  /**
   * List the names of the {@link Dataset}s in this {@code DatasetRepository}.
   * If there is not at least one {@code Dataset} in this repository, an empty
   * list is returned.
   *
   * @return a {@link Collection} of Dataset names ({@link String}s)
   *
   * @since 0.7.0
   */
  Collection<String> list();

  /**
   * Return the {@link URI} of this repository. When used with the {@link
   * org.kitesdk.data.spi.DatasetRepositories#repositoryFor(java.net.URI)}
   * method, an equivalent {@code DatasetRepository} object to {@code this} is
   * returned.
   *
   * @return the {@link URI} of this repository
   *
   * @since 0.12.0
   */
  URI getUri();

}
