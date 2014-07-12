/*
 * Copyright 2014 joey.
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

package org.kitesdk.data.spi;

import com.google.common.base.Preconditions;
import java.net.URI;
import java.util.Map;

public class DatasetRepositories {

  /**
   * Load a {@link DatasetRepository} for the given dataset, view or repository URI.
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
    boolean isRepoUri = URIBuilder.REPO_SCHEME.equals(uri.getScheme());
    Preconditions.checkArgument(isRepoUri ||
        URIBuilder.DATASET_SCHEME.equals(uri.getScheme()) ||
        URIBuilder.VIEW_SCHEME.equals(uri.getScheme()),
        "Not a repository, dataset, or view URI: " + uri);

    Pair<DatasetRepository, Map<String, String>> pair; 
    if (URIBuilder.REPO_SCHEME.equals(uri.getScheme())) {
      pair = Registration.lookupRepoUri(
          URI.create(uri.getRawSchemeSpecificPart()));
    } else {
      pair = Registration.lookupDatasetUri(
          URI.create(uri.getRawSchemeSpecificPart()));
    }

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


}
