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

package org.kitesdk.data;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.AbstractDataset;
import org.kitesdk.data.spi.Constraints;
import org.kitesdk.data.spi.Pair;
import org.kitesdk.data.spi.Registration;

/**
 * Methods for working with {@link Dataset} instances.
 * <p>
 * <b>URIs</b>
 * <p>
 * All methods require a URI that identifies a dataset, view, or
 * repository. The URI must begin with the scheme {@code dataset:}, 
 * {@code view:}, or {@code repo:}. The remainder of the URI is 
 * implementation specific, depending on the dataset scheme.
 * <p>
 * For example, the URI {@code dataset:hive:movies/ratings}
 * references a dataset named <i>ratings</i> in the
 * <i>movies</i> namespace, stored in Hive.
 * <p>
 * The URI {@code view:hive:movies/ratings?year=2015&month=3}
 * references a view of the same <i>ratings</i> dataset. The view
 * is filtered to include records from only March, 2015.
 * <p>
 * See <a href="http://kitesdk.org/docs/current/URIs.html">Dataset and View
 * URIs</a> for the available URI patterns.
 * <p>
 * <b>Dataset Descriptors</b>
 * <p>
 * Some methods require a {@link DatasetDescriptor} that encapsulates metadata
 * about a dataset. Descriptors are built using a
 * {@link DatasetDescriptor.Builder descriptor builder}.
 * <p>
 * <b>Entities</b>
 * <p>
 * <i>Entities</i> are analagous to <i>records</i> in database terminology.
 * The term is used in the API to emphasize that an entity can include not
 * only primitive objects, but also complex objects such as hash maps.
 * <p>
 * Some methods accept an entity class that will be used by Kite when returning
 * entities from a dataset or view.
 *
 * @since 0.8.0
 */
public class Datasets {
  /**
   * Load a {@link Dataset} or {@link View} for the given {@link URI}.
   * <p>
   * URIs must begin with {@code dataset:} or {@code view:}. The remainder of
   * the URI is implementation specific, depending on the dataset scheme.
   * <p>
   * If you use a dataset URI, {@code load} returns the unfiltered dataset.
   * If you use a view URI, {@code load} returns a {@code View} configured to
   * read a subset of the dataset.
   *
   * @param uri a {@code Dataset} or {@code View} URI
   * @param type a Java class that represents an entity in the dataset
   * @param <E> the type used for readers and writers created by this
   *          {@code Dataset}
   * @param <V> the type of {@code View} expected
   * @return a {@code View} for the given URI
   * @throws DatasetNotFoundException if there is no dataset for the given URI
   * @throws NullPointerException if any arguments are {@code null}
   * @throws IllegalArgumentException
   *          if {@code uri} is not a dataset or view URI
   */
  @SuppressWarnings("unchecked")
  public static <E, V extends View<E>> V load(URI uri, Class<E> type) {
    boolean isView = URIBuilder.VIEW_SCHEME.equals(uri.getScheme());
    Preconditions.checkArgument(isView ||
        URIBuilder.DATASET_SCHEME.equals(uri.getScheme()),
        "Not a dataset or view URI: " + uri);
    Preconditions.checkNotNull(type,
        "The entity type can't be null, use Object.class to have the type"
        + " determined by the schema.");

    Pair<DatasetRepository, Map<String, String>> pair =
        Registration.lookupDatasetUri(URI.create(uri.getRawSchemeSpecificPart()));
    DatasetRepository repo = pair.first();
    Map<String, String> uriOptions = pair.second();

    Dataset<E> dataset = repo.load(
        uriOptions.get(URIBuilder.NAMESPACE_OPTION),
        uriOptions.get(URIBuilder.DATASET_NAME_OPTION), type);

    if (isView) {
      return Datasets.<E, V> view(dataset, uriOptions);
    } else {
      // if the URI isn't a view URI, only load the dataset
      return (V) dataset;
    }
  }

  /**
   * Load a {@link Dataset} or {@link View} for the given {@link URI}.
   * <p>
   * URIs must begin with {@code dataset:} or {@code view:}. The remainder of
   * the URI is implementation specific, depending on the dataset scheme.
   * <p>
   * If you use a dataset URI, {@code load} returns the unfiltered dataset.
   * If you use a view URI, {@code load} returns a {@code View} configured to
   * read  a subset of the dataset.
   * 
   * @param uri a {@code Dataset} or {@code View} URI
   * @param <V> the type of {@code View} expected
   * @return a {@code View} for the given URI
   * @throws DatasetNotFoundException if there is no dataset for the given URI
   * @throws NullPointerException if any arguments are {@code null}
   * @throws IllegalArgumentException
   *          if {@code uri} is not a dataset or view URI
   */
  @SuppressWarnings("unchecked")
  public static <V extends View<GenericRecord>> V load(URI uri) {
    return Datasets.<GenericRecord, V>load(uri, GenericRecord.class);
  }

  /**
   * Load a {@link Dataset} or {@link View} for the given {@link URI}.
   * <p>
   * URIs must begin with {@code dataset:} or {@code view:}. The remainder of
   * the URI is implementation specific, depending on the dataset scheme.
   * <p>
   * If you use a dataset URI, {@code load} returns the unfiltered dataset.
   * If you use a view URI, {@code load} returns a {@code View} configured to 
   * read a subset of the dataset.
   *
   * @param uriString a {@code Dataset} or {@code View} URI
   * @param type a Java class that represents an entity in the dataset
   * @param <E> the type used for readers and writers created by this
   *          {@code Dataset}
   * @param <V> the type of {@code View} expected
   * @return a {@code View} for the given URI
   * @throws DatasetNotFoundException if there is no dataset for the given URI
   * @throws NullPointerException if any arguments are {@code null}
   * @throws IllegalArgumentException
   *          if {@code uri} is not a dataset or view URI
   */
  public static <E, V extends View<E>> V load(String uriString, Class<E> type) {
    return Datasets.<E, V> load(URI.create(uriString), type);
  }

  /**
   * Load a {@link Dataset} or {@link View} for the given {@link URI}.
   * <p>
   * URIs must begin with {@code dataset:} or {@code view:}. The remainder of
   * the URI is implementation specific, depending on the dataset scheme.
   * <p>
   * If you use a dataset URI, {@code load} returns the unfiltered dataset.
   * If you use a view URI, {@code load} returns a {@code View} configured to
   * read a subset of the dataset.
   *
   * @param uriString a {@code Dataset} or {@code View} URI
   * @param <V> the type of {@code View} expected
   * @return a {@code View} for the given URI
   * @throws DatasetNotFoundException if there is no dataset for the given URI
   * @throws NullPointerException if any arguments are {@code null}
   * @throws IllegalArgumentException
   *          if {@code uri} is not a dataset or view URI
   */
  public static <V extends View<GenericRecord>> V load(String uriString) {
    return Datasets.<GenericRecord, V>load(
        uriString, GenericRecord.class);
  }

  /**
   * Create a {@link Dataset} for the given dataset or view URI.
   * {@code create} returns an empty dataset. You can use {@code DatasetWriter}
   * to populate your dataset.
   * <p>
   * URIs must begin with {@code dataset:} or {@code view:}. The remainder of
   * the URI is implementation specific, depending on the dataset scheme. If the
   * URI is a view URI, this method creates the underlying dataset and returns a
   * view of it.
   *
   * @param uri a {@code Dataset} or {@code View} URI
   * @param type a Java class that represents an entity in the dataset
   * @param <E> the type used for readers and writers created by this
   *          {@code Dataset}
   * @param <V> the type of {@code Dataset} or {@code View} expected
   * @return a newly created {@code Dataset} responsible for the given URI
   * @throws NullPointerException
   *          if {@code uri}, {@code descriptor}, or {@code type} is
   *          {@code null}
   * @throws IllegalArgumentException
   *          if {@code uri} is not a dataset or view URI
   * @throws DatasetExistsException
   *          if a {@code Dataset} for the given URI already exists
   * @throws IncompatibleSchemaException
   *          if the schema is not compatible with existing datasets with
   *          shared storage (for example, in the same HBase table)
   */
  @SuppressWarnings("unchecked")
  public static <E, V extends View<E>> V create(URI uri, DatasetDescriptor descriptor, Class<E> type) {
    boolean isView = URIBuilder.VIEW_SCHEME.equals(uri.getScheme());
    Preconditions.checkArgument(isView ||
        URIBuilder.DATASET_SCHEME.equals(uri.getScheme()),
        "Not a dataset or view URI: " + uri);
    Preconditions.checkNotNull(type,
        "The entity type can't be null, use Object.class to have the type"
        + " determined by the schema.");

    Pair<DatasetRepository, Map<String, String>> pair =
        Registration.lookupDatasetUri(URI.create(uri.getRawSchemeSpecificPart()));
    DatasetRepository repo = pair.first();
    Map<String, String> uriOptions = pair.second();

    if (descriptor.getLocation() == null && uriOptions.containsKey("location")) {
      descriptor = new DatasetDescriptor.Builder(descriptor)
          .location(uriOptions.get("location"))
          .build();
    }

    Dataset<E> dataset = repo.create(
        uriOptions.get(URIBuilder.NAMESPACE_OPTION),
        uriOptions.get(URIBuilder.DATASET_NAME_OPTION), descriptor, type);

    if (isView) {
      return Datasets.<E, V> view(dataset, uriOptions);
    } else {
      return (V) dataset;
    }
  }

  /**
   * Create a {@link Dataset} for the given dataset or view URI.
   * {@code create} returns an empty dataset. You can use {@code DatasetWriter}
   * to populate your dataset.
   * <p>
   * URIs must begin with {@code dataset:} or {@code view:}. The remainder of
   * the URI is implementation specific, depending on the dataset scheme. If the
   * URI is a view URI, this method creates the underlying dataset and returns a
   * view of it.
   *
   * @param uri a {@code Dataset} or {@code View} URI
   * @param <V> the type of {@code Dataset} or {@code View} expected
   * @return a newly created {@code Dataset} responsible for the given URI
   * @throws NullPointerException
   *          if {@code uri} or {@code descriptor} is {@code null}
   * @throws IllegalArgumentException
   *          if {@code uri} is not a dataset or view URI
   * @throws DatasetExistsException
   *          if a {@code Dataset} for the given URI already exists
   * @throws IncompatibleSchemaException
   *          if the schema is not compatible with existing datasets with
   *          shared storage (for example, in the same HBase table)
   */
  @SuppressWarnings("unchecked")
  public static <V extends View<GenericRecord>> V create(URI uri, DatasetDescriptor descriptor) {
    return Datasets.<GenericRecord, V>create(
        uri, descriptor, GenericRecord.class);
  }

  /**
   * Create a {@link Dataset} for the given dataset or view URI string.
   * {@code create} returns an empty dataset. You can use {@code DatasetWriter}
   * to populate your dataset.
   * <p>
   * URIs must begin with {@code dataset:} or {@code view:}. The remainder of
   * the URI is implementation specific, depending on the dataset scheme. If the
   * URI is a view URI, this method creates the underlying dataset and returns a
   * view of it.
   *
   * @param uri a {@code Dataset} or {@code View} URI string
   * @param type a Java class that represents an entity in the dataset
   * @param <E> the type used for readers and writers created by this
   *          {@code Dataset}
   * @param <V> the type of {@code Dataset} or {@code View} expected
   * @return a newly created {@code Dataset} responsible for the given URI
   * @throws NullPointerException
   *          if {@code uri}, {@code descriptor}, or {@code type} is
   *          {@code null}
   * @throws IllegalArgumentException
   *          if {@code uri} is not a dataset or view URI
   * @throws DatasetExistsException
   *          if a {@code Dataset} for the given URI already exists
   * @throws IncompatibleSchemaException
   *          if the schema is not compatible with existing datasets with
   *          shared storage (for example, in the same HBase table)
   */
  public static <E, V extends View<E>> V create(String uri, DatasetDescriptor descriptor, Class<E> type) {
    return Datasets.<E, V> create(URI.create(uri), descriptor, type);
  }

  /**
   * Create a {@link Dataset} for the given dataset or view URI string.
   * {@code create} returns an empty dataset. You can use {@code DatasetWriter}
   * to populate your dataset.
   * <p>
   * URIs must begin with {@code dataset:} or {@code view:}. The remainder of
   * the URI is implementation specific, depending on the dataset scheme. If the
   * URI is a view URI, this method creates the underlying dataset and returns a
   * view of it.
   *
   * @param uri a {@code Dataset} or {@code View} URI string
   * @param <V> the type of {@code Dataset} or {@code View} expected
   * @return a newly created {@code Dataset} responsible for the given URI
   * @throws NullPointerException
   *          if {@code uri} or {@code descriptor} is {@code null}
   * @throws IllegalArgumentException
   *          if {@code uri} is not a dataset or view URI
   * @throws DatasetExistsException
   *          if a {@code Dataset} for the given URI already exists
   * @throws IncompatibleSchemaException
   *          if the schema is not compatible with existing datasets with
   *          shared storage (for example, in the same HBase table)
   */
  @SuppressWarnings("unchecked")
  public static <V extends View<GenericRecord>> V create(String uri, DatasetDescriptor descriptor) {
    return Datasets.<GenericRecord, V>create(
        uri, descriptor, GenericRecord.class);
  }

  /**
   * Update a {@link Dataset} for the given dataset or view URI.
   * <p>
   * You can add columns, remove columns, or change the data type of columns 
   * in your dataset, provided you don't attempt a change that is incompatible
   * with written data. Avro defines rules for compatible schema evolution. See 
   * <a href="http://kitesdk.org/docs/current/Schema-Evolution.html">Schema
   * Evolution</a>.
   * <p>
   * This method updates the dataset descriptor, so you can also add
   * or change properties.
   * <p>
   * The recommended way to update a dataset descriptor is to build it
   * based on an existing descriptor. Use
   * {@link DatasetDescriptor.Builder(DatasetDescriptor)} to
   * build a DatasetDescriptor based on an existing instance.
   * <p>
   * You cannot change a dataset format or partition strategy.
   * <p>
   * URIs must begin with {@code dataset:}. The remainder of
   * the URI is implementation specific, depending on the dataset scheme.
   *
   * @param uri a {@code Dataset} URI
   * @param type a Java class that represents an entity in the dataset
   * @param <E> the type used for readers and writers created by this
   *          {@code Dataset}
   * @param <D> the type of {@code Dataset} expected
   * @return a {@code Dataset} for the given URI
   * @throws NullPointerException
   *          if {@code uri}, {@code descriptor}, or {@code type} is
   *          {@code null}
   * @throws IllegalArgumentException if {@code uri} is not a dataset URI
   * @throws DatasetNotFoundException
   *          if there is no dataset for the given URI
   * @throws UnsupportedOperationException
   *          if descriptor updates are not supported by the implementation
   * @throws ConcurrentSchemaModificationException
   *          if the {@code Dataset} schema is updated concurrently
   * @throws IncompatibleSchemaException
   *          if the schema is not compatible with previous schemas, or with
   *          existing datasets with shared storage (for example, in the same
   *          HBase table)
   */
  @SuppressWarnings("unchecked")
  public static <E, D extends Dataset<E>> D update(
      URI uri, DatasetDescriptor descriptor, Class<E> type) {
    Preconditions.checkArgument(
        URIBuilder.DATASET_SCHEME.equals(uri.getScheme()),
        "Not a dataset or view URI: " + uri);
    Preconditions.checkNotNull(type,
        "The entity type can't be null, use Object.class to have the type"
            + " determined by the schema.");

    Pair<DatasetRepository, Map<String, String>> pair =
        Registration.lookupDatasetUri(URI.create(uri.getRawSchemeSpecificPart()));
    DatasetRepository repo = pair.first();
    Map<String, String> uriOptions = pair.second();

    return (D) repo.update(
        uriOptions.get(URIBuilder.NAMESPACE_OPTION),
        uriOptions.get(URIBuilder.DATASET_NAME_OPTION), descriptor, type);
  }

  /**
   * Update a {@link Dataset} for the given dataset or view URI.
   * <p>
   * You can add columns, remove columns, or change the data type of columns 
   * in your dataset, provided you don't attempt a change that is incompatible
   * with written data. Avro defines rules for compatible schema evolution. See 
   * <a href="http://kitesdk.org/docs/current/Schema-Evolution.html">Schema
   * Evolution</a>.
   * <p>
   * This method updates the dataset descriptor, so you can also add
   * or change properties.
   * <p>
   * The recommended way to update a dataset descriptor is to build it
   * based on an existing descriptor. Use
   * {@link DatasetDescriptor.Builder(DatasetDescriptor)} to
   * build a DatasetDescriptor based on an existing instance.
   * <p>
   * You cannot change a dataset format or partition strategy.
   * <p>
   * URIs must begin with {@code dataset:}. The remainder of
   * the URI is implementation specific, depending on the dataset scheme.
   *
   * @param uri a {@code Dataset} URI
   * @param <D> the type of {@code Dataset} expected
   * @return a {@code Dataset} for the given URI
   * @throws NullPointerException
   *          if {@code uri} or {@code descriptor} is {@code null}
   * @throws IllegalArgumentException if {@code uri} is not a dataset URI
   * @throws DatasetNotFoundException
   *          if there is no dataset for the given URI
   * @throws UnsupportedOperationException
   *          if descriptor updates are not supported by the implementation
   * @throws ConcurrentSchemaModificationException
   *          if the {@code Dataset} schema is updated concurrently
   * @throws IncompatibleSchemaException
   *          if the schema is not compatible with previous schemas, or with
   *          existing datasets with shared storage (for example, in the same
   *          HBase table)
   */
  @SuppressWarnings("unchecked")
  public static <D extends Dataset<GenericRecord>> D update(
      URI uri, DatasetDescriptor descriptor) {
    return Datasets.<GenericRecord, D>update(
        uri, descriptor, GenericRecord.class);
  }

  /**
   * Update a {@link Dataset} for the given dataset or view URI string.
   * <p>
   * You can add columns, remove columns, or change the data type of columns 
   * in your dataset, provided you don't attempt a change that is incompatible
   * with written data. Avro defines rules for compatible schema evolution. See 
   * <a href="http://kitesdk.org/docs/current/Schema-Evolution.html">Schema
   * Evolution</a>.
   * <p>
   * This method updates the dataset descriptor, so you can also add
   * or change properties.
   * <p>
   * The recommended way to update a dataset descriptor is to build it
   * based on an existing descriptor. Use
   * {@link DatasetDescriptor.Builder(DatasetDescriptor)} to
   * build a DatasetDescriptor based on an existing instance.
   * <p>
   * You cannot change a dataset format or partition strategy.
   * <p>
   * URIs must begin with {@code dataset:}. The remainder of
   * the URI is implementation specific, depending on the dataset scheme.
   *
   * @param uri a {@code Dataset} URI string
   * @param type a Java class that represents an entity in the dataset
   * @param <E> the type used for readers and writers created by this
   *          {@code Dataset}
   * @param <D> the type of {@code Dataset} expected
   * @return a {@code Dataset} for the given URI
   * @throws NullPointerException
   *          if {@code uri}, {@code descriptor}, or {@code type} is
   *          {@code null}
   * @throws IllegalArgumentException if {@code uri} is not a dataset URI
   * @throws DatasetNotFoundException
   *          if there is no dataset for the given URI
   * @throws UnsupportedOperationException
   *          if descriptor updates are not supported by the implementation
   * @throws ConcurrentSchemaModificationException
   *          if the {@code Dataset} schema is updated concurrently
   * @throws IncompatibleSchemaException
   *          if the schema is not compatible with previous schemas, or with
   *          existing datasets with shared storage (for example, in the same
   *          HBase table)
   */
  public static <E, D extends Dataset<E>> D update(String uri, DatasetDescriptor descriptor, Class<E> type) {
    return Datasets.<E, D> update(URI.create(uri), descriptor, type);
  }

  /**
   * Update a {@link Dataset} for the given dataset or view URI string.
   * <p>
   * You can add columns, remove columns, or change the data type of columns 
   * in your dataset, provided you don't attempt a change that is incompatible
   * with written data. Avro defines rules for compatible schema evolution. See 
   * <a href="http://kitesdk.org/docs/current/Schema-Evolution.html">Schema
   * Evolution</a>.
   * <p>
   * This method updates the dataset descriptor, so you can also add
   * or change properties.
   * <p>
   * The recommended way to update a dataset descriptor is to build it
   * based on an existing descriptor. Use
   * {@link DatasetDescriptor.Builder(DatasetDescriptor)} to
   * build a DatasetDescriptor based on an existing instance.
   * <p>
   * You cannot change a dataset format or partition strategy.
   * <p>
   * URIs must begin with {@code dataset:}. The remainder of
   * the URI is implementation specific, depending on the dataset scheme.
   *
   * @param uri a {@code Dataset} URI string
   * @param <D> the type of {@code Dataset} expected
   * @return a {@code Dataset} for the given URI
   * @throws NullPointerException
   *          if {@code uri} or {@code descriptor} is {@code null}
   * @throws IllegalArgumentException if {@code uri} is not a dataset URI
   * @throws DatasetNotFoundException
   *          if there is no dataset for the given URI
   * @throws UnsupportedOperationException
   *          if descriptor updates are not supported by the implementation
   * @throws ConcurrentSchemaModificationException
   *          if the {@code Dataset} schema is updated concurrently
   * @throws IncompatibleSchemaException
   *          if the schema is not compatible with previous schemas, or with
   *          existing datasets with shared storage (for example, in the same
   *          HBase table)
   */
  public static <D extends Dataset<GenericRecord>> D update(String uri, DatasetDescriptor descriptor) {
    return Datasets.<GenericRecord, D>update(
        uri, descriptor, GenericRecord.class);
  }

  /**
   * Delete a {@link Dataset} identified by the given dataset URI.
   * <p>
   * When you call this method using a dataset URI, both data and metadata are
   * deleted. After you call this method, the dataset no longer exists, unless
   * an exception is thrown.
   * <p>
   * When you call this method using a view URI, data in that view is deleted.
   * The dataset's metadata is not changed. This can throw an
   * {@code UnsupportedOperationException} if the delete requires additional
   * work. For example, if some, but not all, of the data in an underlying data
   * file must be removed, then the implementation is allowed to reject the
   * deletion rather than copy the remaining records to a new file.
   * An implementation must document under what conditions it accepts deletes,
   * and under what conditions it rejects them.
   * <p>
   * URIs must begin with {@code dataset:}. The remainder of
   * the URI is implementation specific, depending on the dataset scheme.
   *
   * @param uri a {@code Dataset} URI
   * @return {@code true} if any data or metadata is removed, {@code false}
   *          otherwise
   * @throws NullPointerException if {@code uri} is null
   * @throws IllegalArgumentException if {@code uri} is not a dataset URI
   */
  public static boolean delete(URI uri) {
    Preconditions.checkArgument(
        URIBuilder.DATASET_SCHEME.equals(uri.getScheme()),
        "Not a dataset URI: " + uri);

    Pair<DatasetRepository, Map<String, String>> pair =
        Registration.lookupDatasetUri(URI.create(uri.getRawSchemeSpecificPart()));
    DatasetRepository repo = pair.first();
    Map<String, String> uriOptions = pair.second();

    return repo.delete(
        uriOptions.get(URIBuilder.NAMESPACE_OPTION),
        uriOptions.get(URIBuilder.DATASET_NAME_OPTION));
  }

  /**
   * Delete a {@link Dataset} identified by the given dataset URI string.  
   * <p>
   * When you call this method using a dataset URI, both data and metadata are
   * deleted. After you call this method, the dataset no longer exists, unless
   * an exception is thrown.
   * <p>
   * When you call this method using a view URI, data in that view is deleted.
   * The dataset's metadata is not changed. This can throw an
   * {@code UnsupportedOperationException} if the delete requires additional
   * work. For example, if some, but not all, of the data in an underlying data
   * file must be removed, then the implementation is allowed to reject the
   * deletion rather than copy the remaining records to a new file.
   * An implementation must document under what conditions it accepts deletes,
   * and under what conditions it rejects them.
   * <p>
   * URIs must begin with {@code dataset:}. The remainder of
   * the URI is implementation specific, depending on the dataset scheme.
   *
   * @param uri a {@code Dataset} URI string
   * @return {@code true} if any data or metadata is removed, {@code false}
   *          otherwise
   * @throws NullPointerException if {@code uri} is null
   * @throws IllegalArgumentException if {@code uri} is not a dataset URI
   */
  public static boolean delete(String uri) {
    return delete(URI.create(uri));
  }

  /**
   * Check whether a {@link Dataset} identified by the given URI exists.
   * <p>
   * URIs must begin with {@code dataset:}. The remainder of
   * the URI is implementation specific, depending on the dataset scheme.
   *
   * @param uri a {@code Dataset} URI
   * @return {@code true} if the dataset exists, {@code false} otherwise
   * @throws NullPointerException if {@code uri} is null
   * @throws IllegalArgumentException if {@code uri} is not a dataset URI
   */
  public static boolean exists(URI uri) {
    Preconditions.checkArgument(
        URIBuilder.DATASET_SCHEME.equals(uri.getScheme()),
        "Not a dataset URI: " + uri);

    Pair<DatasetRepository, Map<String, String>> pair =
        Registration.lookupDatasetUri(URI.create(uri.getRawSchemeSpecificPart()));
    DatasetRepository repo = pair.first();
    Map<String, String> uriOptions = pair.second();

    return repo.exists(
        uriOptions.get(URIBuilder.NAMESPACE_OPTION),
        uriOptions.get(URIBuilder.DATASET_NAME_OPTION));
  }

  /**
   * Check whether a {@link Dataset} identified by the given URI string exists.
   * <p>
   * URIs must begin with {@code dataset:}. The remainder of
   * the URI is implementation specific, depending on the dataset scheme.
   *
   * @param uri a {@code Dataset} URI string
   * @return {@code true} if the dataset exists, {@code false} otherwise
   * @throws NullPointerException if {@code uri} is null
   * @throws IllegalArgumentException if {@code uri} is not a dataset URI
   */
  public static boolean exists(String uri) {
    return exists(URI.create(uri));
  }

  /**
   * List the {@link Dataset} URIs in the repository identified by the URI.
   * <p>
   * URI formats are defined by {@code Dataset} implementations. The repository
   * URIs you pass to this method must begin with {@code repo:}. For example, to 
   * list the {@code Dataset} URIs for the Hive repository, provide the URI
   * {@code repo:hive}.
   *
   * @param uri a {@code DatasetRepository} URI
   * @return the URIs present in the {@code DatasetRepository}
   * @throws NullPointerException if {@code uri} is null
   * @throws IllegalArgumentException if {@code uri} is not a repository URI
   */
  public static Collection<URI> list(URI uri) {
    boolean isRepo = URIBuilder.REPO_SCHEME.equals(uri.getScheme());
    Preconditions.checkArgument(isRepo, "Not a repository URI: " + uri);
    DatasetRepository repo = Registration
        .open(URI.create(uri.getRawSchemeSpecificPart()));

    // build a URI for each dataset name
    URI repoUri = repo.getUri();
    List<URI> datasets = Lists.newArrayList();
    for (String namespace : repo.namespaces()) {
      for (String dataset : repo.datasets(namespace)) {
        datasets.add(new URIBuilder(repoUri, namespace, dataset).build());
      }
    }

    return datasets;
  }

  /**
   * List the {@link Dataset} URIs in the repository identified by the URI
   * string.
   * <p>
   * URI formats are defined by {@code Dataset} implementations. The repository
   * URIs you pass to this method must begin with {@code repo:}. For example, to 
   * list the {@code Dataset} URIs for the Hive repository, provide the URI
   * {@code repo:hive}.
   * 
   * @param uri a {@code DatasetRepository} URI string
   * @return the URIs present in the {@code DatasetRepository}
   * @throws NullPointerException if {@code URI} is null
   * @throws IllegalArgumentException if {@code uri} is not a repository URI
   */
  public static Collection<URI> list(String uri) {
    return list(URI.create(uri));
  }

  @SuppressWarnings("unchecked")
  private static <E, V extends View<E>> V view(Dataset<E> dataset,
                                               Map<String, String> uriOptions) {
    if (dataset instanceof AbstractDataset) {
      DatasetDescriptor descriptor = dataset.getDescriptor();
      Schema schema = descriptor.getSchema();
      PartitionStrategy strategy = null;
      if (descriptor.isPartitioned()) {
        strategy = descriptor.getPartitionStrategy();
      }
      Constraints constraints = Constraints.fromQueryMap(
          schema, strategy, uriOptions);
      return (V) ((AbstractDataset) dataset).filter(constraints);
    } else {
      return (V) dataset;
    }
  }
}
