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
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.net.URI;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.avro.Schema;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.RefinableView;
import org.kitesdk.data.View;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Registration {

  private static final Logger LOG = LoggerFactory.getLogger(Registration.class);

  private static final Map<URIPattern, OptionBuilder<DatasetRepository>>
      REPO_BUILDERS = Maps.newLinkedHashMap();

  private static final Map<URIPattern, DatasetBuilder>
      DATASET_BUILDERS = Maps.newLinkedHashMap();

  private static class DatasetBuilder {
    private static final String DATASET_NAME_OPTION = "dataset";
    private final OptionBuilder<DatasetRepository> repoBuilder;

    public DatasetBuilder(OptionBuilder<DatasetRepository> repoBuilder) {
      this.repoBuilder = repoBuilder;
    }

    public <E> Dataset<E> load(Map<String, String> options) {
      DatasetRepository repo = repoBuilder.getFromOptions(options);
      // some URI patterns don't include a dataset as a required option, so
      // check that it is passed as a query option
      Preconditions.checkArgument(options.containsKey(DATASET_NAME_OPTION),
          "Missing required query option \"" + DATASET_NAME_OPTION + "\"");
      return repo.load(options.get(DATASET_NAME_OPTION));
    }
  }

  public static void register(URIPattern repoPattern, URIPattern datasetPattern,
                              OptionBuilder<DatasetRepository> repoBuilder) {
    registerRepoURI(repoPattern, repoBuilder);
    registerDatasetURI(datasetPattern, new DatasetBuilder(repoBuilder));
  }

  /**
   * Registers a {@link URIPattern} and an {@link OptionBuilder} to create
   * instances of {@link DatasetRepository} from the pattern's match options.
   *
   * @param pattern a URIPattern
   * @param builder an OptionBuilder that expects options defined by
   *                {@code pattern} and builds DatasetRepository instances.
   */
  private static void registerRepoURI(URIPattern pattern,
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
  private static void registerDatasetURI(URIPattern pattern,
                                         DatasetBuilder builder) {
    DATASET_BUILDERS.put(pattern, builder);
  }

  @SuppressWarnings("unchecked")
  public static <E, D extends Dataset<E>> D load(URI uri) {
    for (URIPattern pattern : DATASET_BUILDERS.keySet()) {
      Map<String, String> match = pattern.getMatch(uri);
      if (match != null) {
        DatasetBuilder builder = DATASET_BUILDERS.get(pattern);
        Dataset<E> dataset = builder.load(match);
        LOG.debug("Opened dataset {}", dataset);

        return (D) dataset;
      }
    }
    throw new DatasetNotFoundException("Unknown dataset URI: " + uri);
  }

  @SuppressWarnings("unchecked")
  public static <E, V extends View<E>> V view(URI uri) {
    Dataset<E> dataset = null;
    Map<String, String> match = null;
    for (URIPattern pattern : DATASET_BUILDERS.keySet()) {
      match = pattern.getMatch(uri);
      if (match != null) {
        DatasetBuilder builder = DATASET_BUILDERS.get(pattern);
        dataset = builder.load(match);
        LOG.debug("Opened dataset {}", dataset);
        break;
      }
    }
    // match should be null iff dataset is, but check both to be thorough
    if (match == null || dataset == null) {
      throw new DatasetNotFoundException("Unknown dataset URI: " + uri);
    }
    RefinableView<E> view = dataset;
    Schema schema = dataset.getDescriptor().getSchema();
    // for each schema field, see if there is a query arg equality constraint
    for (Schema.Field field : schema.getFields()) {
      String name = field.name();
      if (match.containsKey(name)) {
        view = view.with(name, Conversions.convert(
            match.get(name),
            SchemaUtil.getClassForType(field.schema().getType())));
      }
    }
    return (V) view;
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
