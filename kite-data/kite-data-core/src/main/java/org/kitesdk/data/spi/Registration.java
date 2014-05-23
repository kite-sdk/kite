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
import com.google.common.collect.Maps;
import java.net.URI;
import java.util.Map;
import java.util.ServiceLoader;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Registration {

  private static final Logger LOG = LoggerFactory.getLogger(Registration.class);

  private static final Map<URIPattern, OptionBuilder<DatasetRepository>>
      REPO_BUILDERS = Maps.newLinkedHashMap();

  private static final Map<URIPattern, OptionBuilder<Dataset>>
      DATASET_BUILDERS = Maps.newLinkedHashMap();

  /**
   * Registers a {@link URIPattern} and an {@link OptionBuilder} to create
   * instances of {@link DatasetRepository} from the pattern's match options.
   *
   * @param pattern a URIPattern
   * @param builder an OptionBuilder that expects options defined by
   *                {@code pattern} and builds DatasetRepository instances.
   */
  public static void registerRepoURI(URIPattern pattern,
                                     OptionBuilder<DatasetRepository> builder) {
    REPO_BUILDERS.put(pattern, builder);
  }

  @SuppressWarnings("unchecked")
  public static <R extends DatasetRepository> R open(URI uri) {
    for (URIPattern pattern : REPO_BUILDERS.keySet()) {
      Map<String, String> match = pattern.getMatch(uri);
      if (match != null) {
        OptionBuilder<DatasetRepository> builder = REPO_BUILDERS.get(pattern);
        DatasetRepository repo = builder.getFromOptions(match);
        LOG.debug("Opened repository {}", repo);

        return (R) repo;
      }
    }
    throw new IllegalArgumentException("Unknown repository URI: " + uri);
  }

  /**
   * Registers a {@link URIPattern} and an {@link OptionBuilder} to create
   * instances of {@link Dataset} from the pattern's match options.
   *
   * @param pattern a URIPattern
   * @param builder an OptionBuilder that expects options defined by
   *                {@code pattern} and builds Dataset instances.
   */
  public static void registerDatasetURI(URIPattern pattern,
                                        OptionBuilder<Dataset> builder) {
    DATASET_BUILDERS.put(pattern, builder);
  }

  @SuppressWarnings("unchecked")
  public static <E, D extends Dataset<E>> D load(URI uri) {
    for (URIPattern pattern : DATASET_BUILDERS.keySet()) {
      Map<String, String> match = pattern.getMatch(uri);
      if (match != null) {
        OptionBuilder<Dataset> builder = DATASET_BUILDERS.get(pattern);
        Dataset repo = builder.getFromOptions(match);
        LOG.debug("Opened dataset {}", repo);

        return (D) repo;
      }
    }
    throw new IllegalArgumentException("Unknown dataset URI: " + uri);
  }

  static {
    // load implementations, which will register themselves
    ServiceLoader<Loadable> impls = ServiceLoader.load(Loadable.class);
    for (Loadable loader : impls) {
      // the ServiceLoader is lazy, so this iteration forces service loading
      LOG.debug("Loading: " + loader.getClass().getName());
      loader.load();
    }
    LOG.debug("Registered repository URIs:\n\t" +
        Joiner.on("\n\t").join(REPO_BUILDERS.keySet()));
    LOG.debug("Registered dataset URIs:\n\t" +
        Joiner.on("\n\t").join(DATASET_BUILDERS.keySet()));
  }
}
