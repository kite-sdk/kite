/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.crunch;

import com.google.common.annotations.Beta;
import org.apache.crunch.Target;
import org.apache.crunch.io.ReadableSource;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.View;

/**
 * <p>
 * A helper class for exposing a filesystem-based dataset as a Crunch
 * {@link ReadableSource} or {@link Target}.
 * </p>
 */
@Beta
public class CrunchDatasets {

  /**
   * Expose the given {@link Dataset} as a Crunch {@link ReadableSource}.
   *
   * @param dataset the dataset to read from
   * @param type    the Java type of the entities in the dataset
   * @param <E>     the type of entity produced by the source
   * @return a {@link ReadableSource} for the dataset
   */
  @SuppressWarnings("unchecked")
  public static <E> ReadableSource<E> asSource(Dataset<E> dataset, Class<E> type) {
    return new DatasetSourceTarget<E>(dataset, type);
  }

  /**
   * Expose the given {@link View} as a Crunch {@link ReadableSource}.
   *
   * @param view the view to read from
   * @param type    the Java type of the entities in the dataset
   * @param <E>     the type of entity produced by the source
   * @return a {@link ReadableSource} for the view
   *
   * @since 0.14.0
   */
  public static <E> ReadableSource<E> asSource(View<E> view, Class<E> type) {
    return new DatasetSourceTarget<E>(view, type);
  }

  /**
   * Expose the given {@link Dataset} as a Crunch {@link Target}.
   *
   * @param dataset the dataset to write to
   * @param <E>     the type of entity stored in the dataset
   * @return a {@link Target} for the dataset
   */
  public static <E> Target asTarget(Dataset<E> dataset) {
    return new DatasetTarget<E>(dataset);
  }

  /**
   * Expose the given {@link View} as a Crunch {@link Target}.
   *
   * @param view the view to write to
   * @param <E>  the type of entity stored in the view
   * @return a {@link Target} for the view
   *
   * @since 0.14.0
   */
  public static <E> Target asTarget(View<E> view) {
    return new DatasetTarget<E>(view);
  }
}
