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
import org.kitesdk.data.spi.URIBuilder;

/**
 * <p>Convenience methods for working with {@link Dataset} instances.</p>
 *
 * @since 0.8.0
 */
public class Datasets {

  /**
   * Load a {@link Dataset} or {@link View} for the given {@link URI}.
   * <p>
   * If the URI is a dataset URI, the unfiltered Dataset will be returned.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI.
   * @param type the Java type of the entities in the dataset
   * @param <E> The type of entities stored in the {@code Dataset}.
   * @param <V> The type of {@code View} expected.
   * @return a {@code View} for the given URI.
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
   * If the URI is a dataset URI, the unfiltered Dataset will be returned.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI.
   * @param <V> The type of {@code View} expected.
   * @return a {@code View} for the given URI.
   */
  @SuppressWarnings("unchecked")
  public static <V extends View<GenericRecord>> V load(URI uri) {
    return Datasets.<GenericRecord, V>load(uri, GenericRecord.class);
  }

  /**
   * Load a {@link Dataset} or {@link View} for the given URI string.
   * <p>
   * If the URI is a dataset URI, the unfiltered Dataset will be returned.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uriString a {@code Dataset} or {@code View} URI.
   * @param type the Java type of the entities in the dataset
   * @param <E> The type of entities stored in the {@code Dataset}.
   * @param <V> The type of {@code View} expected.
   * @return a {@code View} for the given URI.
   */
  public static <E, V extends View<E>> V load(String uriString, Class<E> type) {
    return Datasets.<E, V> load(URI.create(uriString), type);
  }

  /**
   * Load a {@link Dataset} or {@link View} for the given URI string.
   * <p>
   * If the URI is a dataset URI, the unfiltered Dataset will be returned.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uriString a {@code Dataset} or {@code View} URI.
   * @param <V> The type of {@code View} expected.
   * @return a {@code View} for the given URI.
   */
  public static <V extends View<GenericRecord>> V load(String uriString) {
    return Datasets.<GenericRecord, V>load(
        uriString, GenericRecord.class);
  }

  /**
   * Load a {@link DatasetRepository} for the given dataset or view URI.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI.
   * @param <R> The type of {@code DatasetRepository} expected.
   * @return a {@code DatasetRepository} responsible for the given URI.
   *
   * @deprecated will be removed in 0.17.0.
   */
  @Deprecated
  public static <R extends DatasetRepository> R repositoryFor(URI uri) {
    return org.kitesdk.data.spi.DatasetRepositories.repositoryFor(uri);
  }

  /**
   * Load a {@link DatasetRepository} for the given dataset or view URI string.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uriString a {@code Dataset} or {@code View} URI.
   * @param <R> The type of {@code DatasetRepository} expected.
   * @return a {@code DatasetRepository} responsible for the given URI.
   *
   * @deprecated will be removed in 0.17.0.
   */
  @Deprecated
  public static <R extends DatasetRepository> R repositoryFor(String uriString) {
    return org.kitesdk.data.spi.DatasetRepositories.repositoryFor(uriString);
  }

  /**
   * Create a {@link Dataset} for the given dataset or view URI.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI.
   * @param type the Java type of the entities in the dataset
   * @param <E> The type of entities stored in the {@code Dataset}.
   * @param <V> The type of {@code Dataset} or {@code View} expected.
   * @return a newly created {@code Dataset} responsible for the given URI.
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

    Dataset<E> dataset = repo.create(
        uriOptions.get(URIBuilder.DATASET_NAME_OPTION), descriptor, type);

    if (isView) {
      return Datasets.<E, V> view(dataset, uriOptions);
    } else {
      return (V) dataset;
    }
  }

  /**
   * Create a {@link Dataset} for the given dataset or view URI.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI.
   * @param <V> The type of {@code Dataset} or {@code View} expected.
   * @return a newly created {@code Dataset} responsible for the given URI.
   */
  @SuppressWarnings("unchecked")
  public static <V extends View<GenericRecord>> V create(URI uri, DatasetDescriptor descriptor) {
    return Datasets.<GenericRecord, V>create(
        uri, descriptor, GenericRecord.class);
  }

  /**
   * Create a {@link Dataset} for the given dataset or view URI string.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI string.
   * @param type the Java type of the entities in the dataset
   * @param <E> The type of entities stored in the {@code Dataset}.
   * @param <V> The type of {@code Dataset} or {@code View} expected.
   * @return a newly created {@code Dataset} responsible for the given URI.
   */
  public static <E, V extends View<E>> V create(String uri, DatasetDescriptor descriptor, Class<E> type) {
    return Datasets.<E, V> create(URI.create(uri), descriptor, type);
  }

  /**
   * Create a {@link Dataset} for the given dataset or view URI string.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI string.
   * @param <V> The type of {@code Dataset} or {@code View} expected.
   * @return a newly created {@code Dataset} responsible for the given URI.
   */
  @SuppressWarnings("unchecked")
  public static <V extends View<GenericRecord>> V create(String uri, DatasetDescriptor descriptor) {
    return Datasets.<GenericRecord, V>create(
        uri, descriptor, GenericRecord.class);
  }

  /**
   * Update a {@link Dataset} for the given dataset or view URI.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI.
   * @param type the Java type of the entities in the dataset
   * @param <E> The type of entities stored in the {@code Dataset}.
   * @param <D> The type of {@code Dataset} expected.
   * @return a newly created {@code Dataset} responsible for the given URI.
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
        uriOptions.get(URIBuilder.DATASET_NAME_OPTION), descriptor, type);
  }

  /**
   * Update a {@link Dataset} for the given dataset or view URI.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI.
   * @param <D> The type of {@code Dataset} expected.
   * @return a newly created {@code Dataset} responsible for the given URI.
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
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI string.
   * @param type the Java type of the entities in the dataset
   * @param <E> The type of entities stored in the {@code Dataset}.
   * @param <D> The type of {@code Dataset} expected.
   * @return a newly created {@code Dataset} responsible for the given URI.
   */
  public static <E, D extends Dataset<E>> D update(String uri, DatasetDescriptor descriptor, Class<E> type) {
    return Datasets.<E, D> update(URI.create(uri), descriptor, type);
  }

  /**
   * Update a {@link Dataset} for the given dataset or view URI string.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI string.
   * @param <D> The type of {@code Dataset} expected.
   * @return a newly created {@code Dataset} responsible for the given URI.
   */
  public static <D extends Dataset<GenericRecord>> D update(String uri, DatasetDescriptor descriptor) {
    return Datasets.<GenericRecord, D>update(
        uri, descriptor, GenericRecord.class);
  }

  /**
   * Delete a {@link Dataset} identified by the given dataset URI.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:".
   *
   * @param uri a {@code Dataset} URI.
   * @return {@code true} if any data or metadata was removed, or {@code false}
   */
  public static boolean delete(URI uri) {
    Preconditions.checkArgument(
        URIBuilder.DATASET_SCHEME.equals(uri.getScheme()),
        "Not a dataset URI: " + uri);

    Pair<DatasetRepository, Map<String, String>> pair =
        Registration.lookupDatasetUri(URI.create(uri.getRawSchemeSpecificPart()));
    DatasetRepository repo = pair.first();
    Map<String, String> uriOptions = pair.second();

    return repo.delete(uriOptions.get(URIBuilder.DATASET_NAME_OPTION));
  }

  /**
   * Delete a {@link Dataset} identified by the given dataset URI string.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:".
   *
   * @param uri a {@code Dataset} URI string.
   * @return {@code true} if any data or metadata was removed, or {@code false}
   */
  public static boolean delete(String uri) {
    return delete(URI.create(uri));
  }

  /**
   * Check if a {@link Dataset} identified by the given URI exists.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:".
   *
   * @param uri a {@code Dataset} URI.
   * @return {@code true} if the dataset exists, {@code false} otherwise
   */
  public static boolean exists(URI uri) {
    Preconditions.checkArgument(
        URIBuilder.DATASET_SCHEME.equals(uri.getScheme()),
        "Not a dataset URI: " + uri);

    Pair<DatasetRepository, Map<String, String>> pair =
        Registration.lookupDatasetUri(URI.create(uri.getRawSchemeSpecificPart()));
    DatasetRepository repo = pair.first();
    Map<String, String> uriOptions = pair.second();

    return repo.exists(uriOptions.get(URIBuilder.DATASET_NAME_OPTION));
  }

  /**
   * Check if a {@link Dataset} identified by the given URI string exists.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:".
   *
   * @param uri a {@code Dataset} URI string.
   * @return {@code true} if the dataset exists, {@code false} otherwise
   */
  public static boolean exists(String uri) {
    return exists(URI.create(uri));
  }

  /**
   * List the {@link Dataset} URIs in the repository identified by the URI
   * <p>
   * URI formats are defined by {@code Dataset} implementations. The repository
   * URIs passed to this method must begin with "repo:".
   *
   * @param uri a {@code DatasetRepository} URI
   * @return the URIs present in the {@code DatasetRepository}
   */
  public static Collection<URI> list(URI uri) {
    boolean isRepo = URIBuilder.REPO_SCHEME.equals(uri.getScheme());
    Preconditions.checkArgument(isRepo, "Not a repository URI: " + uri);
    DatasetRepository repo = Registration
        .open(URI.create(uri.getRawSchemeSpecificPart()));

    // build a URI for each dataset name
    URI repoUri = repo.getUri();
    List<URI> datasets = Lists.newArrayList();
    for (String dataset : repo.list()) {
      datasets.add(new URIBuilder(repoUri, dataset).build());
    }

    return datasets;
  }

  /**
   * List the {@link Dataset} URIs in the repository identified by the URI string
   * <p>
   * URI formats are defined by {@code Dataset} implementations. The repository
   * URIs passed to this method must begin with "repo:".
   *
   * @param uri a {@code DatasetRepository} URI string
   * @return the URIs present in the {@code DatasetRepository}
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
