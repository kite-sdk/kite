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

package org.kitesdk.data.spi;

import com.google.common.base.Preconditions;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.kitesdk.data.Dataset;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.View;

import com.google.common.collect.Maps;

/**
 * Builds dataset and view URIs
 */
public class URIBuilder {
  public static final String DATASET_NAME_OPTION = "dataset";
  public static final String NAMESPACE_OPTION = "namespace";
  public static final String NAMESPACE_DEFAULT = "default";
  public static final String DATASET_SCHEME = "dataset";
  public static final String VIEW_SCHEME = "view";
  public static final String REPO_SCHEME = "repo";

  private URIPattern pattern;
  private boolean isView = false;
  // LinkedHashMap preserves the order so that constructed URIs are more predictable
  private Map<String, String> options = Maps.newLinkedHashMap();

  /**
   * Constructs a builder based on the given repository URI and {@link Dataset#getName() dataset name}.
   *
   * @param repoUri the {@link DatasetRepository} URI
   * @param namespace A namespace, or logical group name, for the dataset.
   * @param datasetName the {@link Dataset} name
   */
  public URIBuilder(String repoUri, String namespace, String datasetName) {
    this(URI.create(repoUri), namespace, datasetName);
  }

  /**
   * Constructs a builder based on the given repository URI and {@link Dataset#getName() dataset name}.
   *
   * @param repoUri the {@link DatasetRepository} URI
   * @param namespace A namespace, or logical group name, for the dataset.
   * @param datasetName the {@link Dataset} name
   */
  public URIBuilder(URI repoUri, String namespace, String datasetName) {
    Preconditions.checkNotNull(repoUri, "Repository URI cannot be null");
    Preconditions.checkNotNull(datasetName, "Dataset name cannot be null");
    Preconditions.checkArgument(REPO_SCHEME.equals(repoUri.getScheme()));

    Pair<URIPattern, Map<String, String>> pair = Registration
        .lookupPatternByRepoUri(URI.create(repoUri.getRawSchemeSpecificPart()));
    this.pattern = pair.first();
    options.putAll(pair.second());
    options.put(DATASET_NAME_OPTION, datasetName);
    options.put(NAMESPACE_OPTION, namespace);
  }

  public URIBuilder(String uri) {
    this(URI.create(uri));
  }

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
   * @return this builder
   */
  public URIBuilder with(String name, Object value) {
    options.put(name, Conversions.makeString(value));
    this.isView = true;
    return this;
  }

  /**
   * Returns the URI encompassing the given constraints. The referenced
   * {@link Dataset} or {@link View} may be loaded again with
   * {@link Datasets#load(java.net.URI, java.lang.Class)}.
   *
   * @return the URI
   */
  public URI build() {
    try {
      return new URI(isView ? VIEW_SCHEME : DATASET_SCHEME,
          pattern.construct(options).toString(), null);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Could not build URI", e);
    }
  }
}
