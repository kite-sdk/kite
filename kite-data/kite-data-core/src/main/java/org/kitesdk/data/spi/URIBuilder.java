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
  private URI repoUri;
  private String datasetName;
  // LinkedHashMap preserves the order so that constructed URIs are more predictable
  private Map<String, String> equalityConstraints = Maps.newLinkedHashMap();

  /**
   * Constructs a builder based on the given repository URI and {@link Dataset#getName() dataset name}.
   *
   * @param repoUri the {@link DatasetRepository} URI
   * @param datasetName the {@link Dataset} name
   */
  public URIBuilder(String repoUri, String datasetName) {
    this(URI.create(repoUri), datasetName);
  }

  /**
   * Constructs a builder based on the given repository URI and {@link Dataset#getName() dataset name}.
   *
   * @param repoUri the {@link DatasetRepository} URI
   * @param datasetName the {@link Dataset} name
   */
  public URIBuilder(URI repoUri, String datasetName) {
    Preconditions.checkNotNull(repoUri, "Repository URI cannot be null");
    Preconditions.checkNotNull(datasetName, "Dataset name cannot be null");
    this.repoUri = repoUri;
    this.datasetName = datasetName;
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
    equalityConstraints.put(name, Conversions.makeString(value));
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
    URI repoStorageUri = URI.create(repoUri.getRawSchemeSpecificPart());
    Pair<URIPattern, Map<String, String>> pair =
        Registration.lookupPatternByRepoUri(repoStorageUri);
    Map<String, String> uriData = pair.second();
    uriData.put("dataset", datasetName);
    uriData.putAll(equalityConstraints);
    try {
      return new URI(
          equalityConstraints.isEmpty() ? "dataset" : "view",
          pair.first().construct(uriData).toString(),
          null);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Could not build URI", e);
    }
  }
}
