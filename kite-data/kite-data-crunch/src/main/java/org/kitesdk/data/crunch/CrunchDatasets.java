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
   * Only the FileSystem {@code Dataset} implementation is supported and the
   * file format must be {@code Formats.PARQUET} or {@code Formats.AVRO}.
   *
   * @param dataset the dataset to read from
   * @param type    the Java type of the entities in the dataset
   * @param <E>     the type of entity produced by the source
   * @return the {@link ReadableSource}, or <code>null</code> if the dataset is not
   * filesystem-based.
   */
  @SuppressWarnings("unchecked")
  public static <E> ReadableSource<E> asSource(Dataset<E> dataset, Class<E> type) {
    return new DatasetSourceTarget<E>(dataset, type);
  }

  /**
   * Expose the given {@link Dataset} as a Crunch {@link Target}.
   *
   * Only the FileSystem {@code Dataset} implementation is supported and the
   * file format must be {@code Formats.PARQUET} or {@code Formats.AVRO}. In
   * addition, the given {@code Dataset} must not be partitioned,
   * <strong>or</strong> must be a leaf partition in the partition hierarchy.
   *
   * <strong>The {@code Target} returned by this method will not write to
   * sub-partitions.</strong>
   *
   * @param dataset the dataset to write to
   * @return the {@link Target}, or <code>null</code> if the dataset is not
   * filesystem-based.
   */
  public static <E> Target asTarget(Dataset<E> dataset) {
    return new DatasetTarget<E>(dataset);
  }
}
