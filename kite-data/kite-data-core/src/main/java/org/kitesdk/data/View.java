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

import java.net.URI;
import javax.annotation.concurrent.Immutable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

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
   * Get the runtime type of entities contained in this {@code View}.
   *
   * @return the runtime type of entities contained in this view
   *
   * @since 0.15.0
   */
  public Class<E> getType();

  /**
   * Get the schema of entities contained in this {@code View}.
   *
   * @return the schema of entities contained in this view
   *
   * @since 1.1.0
   */
  public Schema getSchema();

  /**
   * Check whether this {@link View} contains any records.
   *
   * Implementations should return once a single record in this view is found.
   * But this might require scanning through large amounts of data sequentially
   * to determine if there are any records.
   *
   * @return {@code true} if least one record exists, {@code false} otherwise
   * @since 0.15.0
   */
  public boolean isEmpty();

  /**
   * Get a {@link URI} that identifies this {@code View}.
   *
   * @return a {@link URI} for this view.
   *
   * @since 0.16.0
   */
  public URI getUri();

  /**
   * Returns an {@code Iterable} of non-overlapping {@link View} objects that
   * partition the underlying {@link Dataset} and cover this {@code View}.
   *
   * The returned {@code View} objects are implementation-specific, but should
   * represent reasonable partitions of the underlying {@code Dataset} based on
   * its layout.
   *
   * The data contained by the union of each {@code View} in the
   * {@code Iterable} must be a super-set of this {@code View}.
   *
   * Note that partitions are actual partitions under which data is stored.
   * Implementations should omit any {@code View} that is empty.
   *
   * @return
   *      An Iterable of the {@code View} that cover this {@code View}.
   * @throws IllegalStateException
   *      If the underlying {@code Dataset} is not partitioned.
   * @throws UnsupportedOperationException
   *      If the implementation does not support covering partitions.
   * @since 1.1.0
   */
  public Iterable<PartitionView<E>> getCoveringPartitions();

  /**
   * Creates a copy of this {@code View} that projects entities to the given
   * {@link Schema}.
   * <p>
   * This method always returns a {@code View} with type {@link GenericRecord}.
   *
   * @param schema an Avro schema to project entities to
   * @return a copy of this view that projects entities to the given schema
   * @throws IncompatibleSchemaException
   *          If the given {@code schema} is incompatible with the underlying
   *          dataset.
   *
   * @since 1.1.0
   */
  View<GenericRecord> asSchema(Schema schema);

  /**
   * Creates a copy of this {@code View} that reads and writes entities of the
   * given type class.
   *
   * @param <T> the type of entities that will be read or written by this view
   * @param type an entity class to use
   * @return a copy of this view that projects entities to the given type
   * @throws IncompatibleSchemaException
   *          If the given {@code type} is incompatible with the underlying
   *          dataset.
   *
   * @since 1.1.0
   */
  <T> View<T> asType(Class<T> type);
}
