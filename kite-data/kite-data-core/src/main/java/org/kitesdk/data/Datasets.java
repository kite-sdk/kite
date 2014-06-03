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

import java.net.URI;
import org.kitesdk.data.spi.Registration;

public class Datasets {

  private static final String DATASET_SCHEME = "dataset";
  private static final String VIEW_SCHEME = "view";

  public static <E, D extends Dataset<E>> D load(URI uri) {
    if (DATASET_SCHEME.equals(uri.getScheme())) {
      // accept dataset:<storage-uri> URIs
      return Registration.<E, D>load(URI.create(uri.getRawSchemeSpecificPart()));
    } else {
      return Registration.<E, D>load(uri);
    }
  }

  public static <E, D extends Dataset<E>> D load(String uriString) {
    return Datasets.<E, D>load(URI.create(uriString));
  }

  public static <E, V extends View<E>> V view(URI uri) {
    if (VIEW_SCHEME.equals(uri.getScheme())) {
      // accept view:<storage-uri> URIs
      return Registration.<E, V>view(URI.create(uri.getRawSchemeSpecificPart()));
    } else {
      return Registration.<E, V>view(uri);
    }
  }

  public static <E, V extends View<E>> V view(String uriString) {
    return Datasets.<E, V>view(URI.create(uriString));
  }
}
