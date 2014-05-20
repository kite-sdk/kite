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
import com.google.common.collect.Maps;
import java.net.URI;
import java.util.Map;
import org.kitesdk.data.spi.OptionBuilder;
import org.kitesdk.data.spi.URIPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Datasets {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetRepositories.class);

  private static final URIPattern BASE_PATTERN = new URIPattern(
      URI.create("dataset:*storage-uri"));

  public static final Map<URIPattern, OptionBuilder<Dataset>> REGISTRY =
      Maps.newLinkedHashMap();

  public static void register(URIPattern pattern, OptionBuilder<Dataset> builder) {
    REGISTRY.put(pattern, builder);
  }

  public static <E> Dataset<E> load(URI uri) {
    final Map<String, String> baseMatch = BASE_PATTERN.getMatch(uri);

    Preconditions.checkArgument(baseMatch != null,
        "Invalid dataset URI \"%s\": scheme must be 'dataset'", uri);

    final URI storage = URI.create(baseMatch.get("storage-uri"));
    Map<String, String> match;

    for (URIPattern pattern : REGISTRY.keySet()) {
      match = pattern.getMatch(storage);
      if (match != null) {
        OptionBuilder<Dataset> builder = REGISTRY.get(pattern);
        Dataset dataset = builder.getFromOptions(match);
        LOG.debug("Loaded dataset \"{}\" from uri \"{}\"", dataset, uri);

        return dataset;
      }
    }

    throw new IllegalArgumentException("Unknown storage URI:" + storage);
  }

}
