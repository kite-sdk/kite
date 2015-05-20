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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import org.kitesdk.data.spi.Constraints;
import org.kitesdk.data.spi.Conversions;
import org.kitesdk.data.spi.Pair;
import org.kitesdk.data.spi.Registration;
import org.kitesdk.data.spi.URIPattern;

/**
 * Builds dataset and view URIs
 *
 * @since 0.17.0
 */
public class URIBuilder {
  public static final String DATASET_NAME_OPTION = "dataset";
  public static final String NAMESPACE_OPTION = "namespace";
  public static final String NAMESPACE_DEFAULT = "default";
  public static final String DATASET_SCHEME = "dataset";
  public static final String VIEW_SCHEME = "view";
  public static final String REPO_SCHEME = "repo";

  /**
   * Builds a dataset URI from the given repository URI string, namespace, and
   * dataset name.
   *
   * @param repoUri a repository URI string
   * @param namespace a String namespace
   * @param dataset a String dataset name
   * @return a dataset URI for the namespace and dataset name in the repository
   *
   * @since 0.17.0
   */
  public static URI build(String repoUri, String namespace, String dataset) {
    return build(URI.create(repoUri), namespace, dataset);
  }

  /**
   * Builds a dataset URI from the given repository URI, namespace, and dataset
   * name.
   *
   * @param repoUri a repository URI
   * @param namespace a String namespace
   * @param dataset a String dataset name
   * @return a dataset URI for the namespace and dataset name in the repository
   *
   * @since 0.17.0
   */
  public static URI build(URI repoUri, String namespace, String dataset) {
    return new URIBuilder(repoUri, namespace, dataset).build();
  }

  private URIPattern pattern;
  private boolean isView = false;
  // LinkedHashMap preserves the order so that constructed URIs are more predictable
  private Map<String, String> options = Maps.newLinkedHashMap();

  /**
   * Constructs a builder based on the given repository URI and
   * {@link Dataset#getName() dataset name}.
   *
   * @param repoUri the repository URI
   * @param namespace A namespace, or logical group name, for the dataset.
   * @param datasetName the {@link Dataset} name
   *
   * @since 0.17.0
   */
  public URIBuilder(String repoUri, String namespace, String datasetName) {
    this(URI.create(repoUri), namespace, datasetName);
  }

  /**
   * Constructs a builder based on the given repository URI and
   * {@link Dataset#getName() dataset name}.
   *
   * @param repoUri the repository URI
   * @param namespace A namespace, or logical group name, for the dataset.
   * @param dataset the {@link Dataset} name
   *
   * @since 0.17.0
   */
  public URIBuilder(URI repoUri, String namespace, String dataset) {
    Preconditions.checkNotNull(repoUri, "Repository URI cannot be null");
    Preconditions.checkNotNull(dataset, "Dataset name cannot be null");
    Preconditions.checkArgument(REPO_SCHEME.equals(repoUri.getScheme()),
        "Repository URI must start with \"repo:\" but was:" + repoUri);

    Pair<URIPattern, Map<String, String>> pair = Registration
        .lookupPatternByRepoUri(URI.create(repoUri.getRawSchemeSpecificPart()));
    this.pattern = pair.first();
    options.putAll(pair.second());
    options.put(DATASET_NAME_OPTION, dataset);
    options.put(NAMESPACE_OPTION, namespace);
  }

  /**
   * Constructs a builder based on the given dataset or view URI string.
   *
   * @param uri a dataset or view URI String
   */
  public URIBuilder(String uri) {
    this(URI.create(uri));
  }

  /**
   * Constructs a builder based on the given dataset or view URI.
   *
   * @param uri a dataset or view URI
   *
   * @since 0.17.0
   */
  public URIBuilder(URI uri) {
    Preconditions.checkNotNull(uri, "URI cannot be null");
    boolean isViewUri = VIEW_SCHEME.equals(uri.getScheme());
    Preconditions.checkArgument(isViewUri ||
        DATASET_SCHEME.equals(uri.getScheme()),
        "Not a dataset or view URI: " + uri);

    Pair<URIPattern, Map<String, String>> pair = Registration
        .lookupDatasetPattern(URI.create(uri.getRawSchemeSpecificPart()));
    this.pattern = pair.first();
    this.isView = isViewUri;
    options.put(NAMESPACE_OPTION, NAMESPACE_DEFAULT);
    options.putAll(pair.second());
  }

  /**
   * Adds a set of constraints.
   *
   * @param constraints a set of constraints
   * @return this builder for method chaining
   */
  @VisibleForTesting
  URIBuilder constraints(Constraints constraints) {
    options.putAll(constraints.toQueryMap());
    this.isView = !constraints.isUnbounded();
    return this;
  }

  /**
   * Adds a view constraint equivalent to
   * {@link org.kitesdk.data.RefinableView#with(String, Object...)}
   *
   * @param name the field name of the Entity
   * @param value the field value
   * @return this builder for method chaining
   *
   * @since 0.17.0
   */
  public URIBuilder with(String name, Object value) {
    options.put(name, Conversions.makeString(value));
    this.isView = true;
    return this;
  }

  /**
   * Sets the namespace that is used to construct the dataset URI.
   *
   * @param namespace a String namespace
   * @return this builder for method chaining
   *
   * @since 0.17.0
   */
  public URIBuilder namespace(String namespace) {
    options.put(NAMESPACE_OPTION, namespace);
    return this;
  }

  /**
   * Sets the dataset name that is used to construct the dataset URI.
   *
   * @param dataset a String dataset name
   * @return this builder for method chaining
   *
   * @since 0.17.0
   */
  public URIBuilder dataset(String dataset) {
    options.put(DATASET_NAME_OPTION, dataset);
    return this;
  }

  /**
   * Returns a dataset or view URI encompassing the given constraints. The
   * referenced {@link Dataset} or {@link View} may be loaded again with
   * {@link Datasets#load(URI, Class)}.
   *
   * @return a dataset or view URI
   *
   * @since 0.17.0
   */
  public URI build() {
    try {
      return new URI((isView ? VIEW_SCHEME : DATASET_SCHEME) + ":" +
          pattern.construct(options).toString());
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Could not build URI", e);
    }
  }
}
