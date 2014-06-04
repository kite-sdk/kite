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
import org.kitesdk.data.spi.Registration;

public class Datasets {

  private static final String DATASET_SCHEME = "dataset";
  private static final String VIEW_SCHEME = "view";

  /**
   * Load a {@link Dataset} for the given {@link URI}.
   * <p>
   * If the URI is a view URI, the underlying Dataset will be returned.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI.
   * @param <E> The type of entities stored in the {@code Dataset}.
   * @param <D> The type of {@code Dataset} expected.
   * @return a {@code Dataset} for the given URI.
   */
  public static <E, D extends Dataset<E>> D load(URI uri) {
    Preconditions.checkArgument(
        VIEW_SCHEME.equals(uri.getScheme()) ||
        DATASET_SCHEME.equals(uri.getScheme()),
        "Not a dataset or view URI: " + uri);
    return Registration.<E, D>load(URI.create(uri.getRawSchemeSpecificPart()));
  }

  /**
   * Load a {@link Dataset} for the given URI string.
   * <p>
   * If the URI is a view URI, the underlying Dataset will be returned.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uriString a {@code Dataset} or {@code View} URI.
   * @param <E> The type of entities stored in the {@code Dataset}.
   * @param <D> The type of {@code Dataset} expected.
   * @return a {@code Dataset} for the given URI.
   */
  public static <E, D extends Dataset<E>> D load(String uriString) {
    return Datasets.<E, D>load(URI.create(uriString));
  }

  /**
   * Load a {@link View} for the given {@link URI}.
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
  public static <E, V extends View<E>> V view(URI uri) {
    Preconditions.checkArgument(
        VIEW_SCHEME.equals(uri.getScheme()) ||
            DATASET_SCHEME.equals(uri.getScheme()),
        "Not a dataset or view URI: " + uri);
    return Registration.<E, V>view(URI.create(uri.getRawSchemeSpecificPart()));
  }

  /**
   * Load a {@link View} for the given URI string.
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
  public static <E, V extends View<E>> V view(String uriString) {
    return Datasets.<E, V>view(URI.create(uriString));
  }
}
