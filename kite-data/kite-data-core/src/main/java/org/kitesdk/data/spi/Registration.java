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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.kitesdk.data.DatasetNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Registration {

  private static final Logger LOG = LoggerFactory.getLogger(Registration.class);

  private static final Map<URIPattern, OptionBuilder<DatasetRepository>>
      REPO_PATTERNS = Maps.newLinkedHashMap();

  private static final Map<URIPattern, OptionBuilder<DatasetRepository>>
      DATASET_PATTERNS = Maps.newLinkedHashMap();

  private static final Map<URIPattern, URIPattern>
      REPO_TO_DATASET_PATTERNS = Maps.newLinkedHashMap();

  /**
   * Registers a repository and a dataset {@link URIPattern} using a repository
   * {@link OptionBuilder}.
   * <p>
   * Both patterns should produce options used by the builder to produce a
   * {@link DatasetRepository}. The dataset pattern should also produce a
   * "dataset" option that will be passed to the repository's load method as
   * the dataset name.
   *
   * @param repoPattern a {@code URIPattern} for a {@code DatasetRepository}
   * @param datasetPattern a {@code URIPattern} for a {@code Dataset}
   * @param repoBuilder a {@code OptionBuilder} valid for both URIs
   */
  public static void register(URIPattern repoPattern, URIPattern datasetPattern,
                              OptionBuilder<DatasetRepository> repoBuilder) {
    REPO_PATTERNS.put(repoPattern, repoBuilder);
    DATASET_PATTERNS.put(datasetPattern, repoBuilder);
    // the first dataset pattern for a repository pattern is always used
    if (!REPO_TO_DATASET_PATTERNS.containsKey(repoPattern)) {
      REPO_TO_DATASET_PATTERNS.put(repoPattern, datasetPattern);
    }
  }

  public static Pair<URIPattern, Map<String, String>> lookupPatternByRepoUri(URI uri) {
    for (URIPattern pattern : REPO_TO_DATASET_PATTERNS.keySet()) {
      Map<String, String> match = pattern.getMatch(uri);
      if (match != null) {
        URIPattern datasetPattern = REPO_TO_DATASET_PATTERNS.get(pattern);
        return Pair.of(datasetPattern, match);
      }
    }
    throw new IllegalArgumentException("Unknown repository URI pattern: dataset:" + uri);
  }

  public static Pair<URIPattern, Map<String, String>> lookupDatasetPattern(URI uri) {
    for (URIPattern pattern : DATASET_PATTERNS.keySet()) {
      Map<String, String> match = pattern.getMatch(uri);
      if (match != null) {
        return Pair.of(pattern, match);
      }
    }
    throw new IllegalArgumentException("Unknown dataset URI pattern: dataset:" + uri);
  }

  public static Pair<DatasetRepository, Map<String, String>>
      lookupRepoUri(URI repoUri) {
    for (URIPattern pattern : REPO_PATTERNS.keySet()) {
      Map<String, String> match = pattern.getMatch(repoUri);
      if (match != null) {
        return Pair.of(REPO_PATTERNS.get(pattern).getFromOptions(match), match);
      }
    }
    throw new IllegalArgumentException("Unknown repository URI pattern: repo:" + repoUri);
  }

  @SuppressWarnings("unchecked")
  public static <R extends DatasetRepository> R open(URI uri) {
    return (R) lookupRepoUri(uri).first();
  }

  public static Pair<DatasetRepository, Map<String, String>>
      lookupDatasetUri(URI datasetUri) {
    String scheme = datasetUri.getScheme();
    List<String> schemeMatches = Lists.newArrayList();

    for (URIPattern pattern : DATASET_PATTERNS.keySet()) {
      Map<String, String> match = pattern.getMatch(datasetUri);
      if (match != null) {
        return Pair.of(DATASET_PATTERNS.get(pattern).getFromOptions(match), match);
      } else if (pattern.getScheme() != null && pattern.getScheme().equals(scheme)) {
        schemeMatches.add(pattern.getPatternString());
      }
    }

    String message = "Unknown dataset URI pattern: dataset:" + datasetUri;
    if (schemeMatches.isEmpty()) {
      // no known patterns for the scheme, maybe jars are missing
      message += "\nCheck that JARs for " + scheme +
          " datasets are on the classpath";
    } else {
      // show the known patterns in case it's a simple error
      message += "\nKnown patterns for " + scheme + ":\n  dataset:" +
          Joiner.on("\n  dataset:").join(schemeMatches);
    }

    throw new DatasetNotFoundException(message);
  }

  static {
    // load implementations, which will register themselves
    ServiceLoader<Loadable> impls = ServiceLoader.load(Loadable.class);
    for (Loadable loader : impls) {
      // the ServiceLoader is lazy, so this iteration forces service loading
      LOG.debug("Loading: " + loader.getClass().getName());
      try {
        loader.load();
      } catch (Exception e) {
        if (LOG.isDebugEnabled()) {
          LOG.warn("Not loading URI patterns in " + loader.getClass().getName(), e);
        } else {
          LOG.warn("Not loading URI patterns in " + loader.getClass().getName());
        }
      }
    }
    LOG.debug("Registered repository URIs:\n\t" +
        Joiner.on("\n\t").join(REPO_PATTERNS.keySet()));
    LOG.debug("Registered dataset URIs:\n\t" +
        Joiner.on("\n\t").join(DATASET_PATTERNS.keySet()));
  }
}
