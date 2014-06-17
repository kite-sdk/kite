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
import java.net.URI;
import java.util.Map;
import org.apache.avro.Schema;
import org.kitesdk.data.spi.Conversions;
import org.kitesdk.data.spi.Pair;
import org.kitesdk.data.spi.Registration;
import org.kitesdk.data.spi.SchemaUtil;

public class Datasets {

  private static final String DATASET_NAME_OPTION = "dataset";
  private static final String DATASET_SCHEME = "dataset";
  private static final String VIEW_SCHEME = "view";

  /**
   * Load a {@link Dataset} or {@link View} for the given {@link URI}.
   * <p>
   * If the URI is a dataset URI, the unfiltered Dataset will be returned.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI.
   * @param <E> The type of entities stored in the {@code Dataset}.
   * @param <V> The type of {@code View} expected.
   * @return a {@code View} for the given URI.
   */
  @SuppressWarnings("unchecked")
  public static <E, V extends View<E>> V load(URI uri) {
    boolean isView = VIEW_SCHEME.equals(uri.getScheme());
    Preconditions.checkArgument(isView ||
        DATASET_SCHEME.equals(uri.getScheme()),
        "Not a dataset or view URI: " + uri);

    Pair<DatasetRepository, Map<String, String>> pair =
        Registration.lookupDatasetUri(URI.create(uri.getRawSchemeSpecificPart()));
    DatasetRepository repo = pair.first();
    Map<String, String> uriOptions = pair.second();

    Dataset<E> dataset = repo.load(uriOptions.get(DATASET_NAME_OPTION));

    if (isView) {
      RefinableView<E> view = dataset;
      Schema schema = dataset.getDescriptor().getSchema();
      // for each schema field, see if there is a query arg equality constraint
      for (Schema.Field field : schema.getFields()) {
        String name = field.name();
        if (uriOptions.containsKey(name)) {
          view = view.with(name, Conversions.convert(
              uriOptions.get(name),
              SchemaUtil.getClassForType(field.schema().getType())));
        }
      }
      return (V) view;

    } else {
      // if the URI isn't a view URI, only load the dataset
      return (V) dataset;
    }
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
   * @param <E> The type of entities stored in the {@code Dataset}.
   * @param <V> The type of {@code View} expected.
   * @return a {@code View} for the given URI.
   */
  public static <E, V extends View<E>> V load(String uriString) {
    return Datasets.<E, V> load(URI.create(uriString));
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
   */
  @SuppressWarnings("unchecked")
  public static <R extends DatasetRepository> R repositoryFor(URI uri) {
    Preconditions.checkArgument(
        DATASET_SCHEME.equals(uri.getScheme()) ||
        VIEW_SCHEME.equals(uri.getScheme()),
        "Not a dataset or view URI: " + uri);

    Pair<DatasetRepository, Map<String, String>> pair =
        Registration.lookupDatasetUri(URI.create(uri.getRawSchemeSpecificPart()));

    return (R) pair.first();
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
   */
  public static <R extends DatasetRepository> R repositoryFor(String uriString) {
    return repositoryFor(URI.create(uriString));
  }

  /**
   * Create a {@link Dataset} for the given dataset or view URI.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI.
   * @param <E> The type of entities stored in the {@code Dataset}.
   * @param <D> The type of {@code Dataset} expected.
   * @return a newly created {@code Dataset} responsible for the given URI.
   */
  @SuppressWarnings("unchecked")
  public static <E, D extends Dataset<E>> D create(
      URI uri, DatasetDescriptor descriptor) {
    Preconditions.checkArgument(
        DATASET_SCHEME.equals(uri.getScheme()) ||
            VIEW_SCHEME.equals(uri.getScheme()),
        "Not a dataset or view URI: " + uri);

    Pair<DatasetRepository, Map<String, String>> pair =
        Registration.lookupDatasetUri(URI.create(uri.getRawSchemeSpecificPart()));
    DatasetRepository repo = pair.first();
    Map<String, String> uriOptions = pair.second();

    return (D) repo.create(uriOptions.get(DATASET_NAME_OPTION), descriptor);
  }

  /**
   * Create a {@link Dataset} for the given dataset or view URI string.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI string.
   * @param <E> The type of entities stored in the {@code Dataset}.
   * @param <D> The type of {@code Dataset} expected.
   * @return a newly created {@code Dataset} responsible for the given URI.
   */
  public static <E, D extends Dataset<E>> D create(String uri, DatasetDescriptor descriptor) {
    return Datasets.<E, D> create(URI.create(uri), descriptor);
  }

  /**
   * Create a {@link Dataset} for the given dataset or view URI.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI.
   * @param <E> The type of entities stored in the {@code Dataset}.
   * @param <D> The type of {@code Dataset} expected.
   * @return a newly created {@code Dataset} responsible for the given URI.
   */
  @SuppressWarnings("unchecked")
  public static <E, D extends Dataset<E>> D update(
      URI uri, DatasetDescriptor descriptor) {
    Preconditions.checkArgument(
        DATASET_SCHEME.equals(uri.getScheme()) ||
            VIEW_SCHEME.equals(uri.getScheme()),
        "Not a dataset or view URI: " + uri);

    Pair<DatasetRepository, Map<String, String>> pair =
        Registration.lookupDatasetUri(URI.create(uri.getRawSchemeSpecificPart()));
    DatasetRepository repo = pair.first();
    Map<String, String> uriOptions = pair.second();

    return (D) repo.update(uriOptions.get(DATASET_NAME_OPTION), descriptor);
  }

  /**
   * Create a {@link Dataset} for the given dataset or view URI string.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI string.
   * @param <E> The type of entities stored in the {@code Dataset}.
   * @param <D> The type of {@code Dataset} expected.
   * @return a newly created {@code Dataset} responsible for the given URI.
   */
  public static <E, D extends Dataset<E>> D update(String uri, DatasetDescriptor descriptor) {
    return Datasets.<E, D> update(URI.create(uri), descriptor);
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
        DATASET_SCHEME.equals(uri.getScheme()),
        "Not a dataset URI: " + uri);

    Pair<DatasetRepository, Map<String, String>> pair =
        Registration.lookupDatasetUri(URI.create(uri.getRawSchemeSpecificPart()));
    DatasetRepository repo = pair.first();
    Map<String, String> uriOptions = pair.second();

    return repo.delete(uriOptions.get(DATASET_NAME_OPTION));
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
}
