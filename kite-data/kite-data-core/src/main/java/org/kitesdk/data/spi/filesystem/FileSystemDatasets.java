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

package org.kitesdk.data.spi.filesystem;

import java.net.URI;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.View;

/**
 * <p>
 * A helper class working with {@link FileSystemDataset} instances.
 * </p>
 */
public class FileSystemDatasets {

  /**
   * Convert a URI for a partition directory in a filesystem dataset to a {@link View}
   * object representing that partition.
   * @param dataset the (partitioned) filesystem dataset
   * @param uri the path to the partition directory
   * @return a view of the partition
   */
  public static <E> View<E> viewForUri(Dataset<E> dataset, URI uri) {
    if (dataset instanceof FileSystemDataset) {
      return ((FileSystemDataset<E>) dataset).viewForUri(uri);
    }
    throw new IllegalArgumentException("Not a file system dataset: " + dataset);
  }

  /**
   * Convert a URI for a partition directory in a filesystem dataset to a {@link View}
   * object representing that partition.
   * @param dataset the (partitioned) filesystem dataset
   * @param uri the path to the partition directory
   * @return a view of the partition
   */
  public static <E> View<E> viewForUri(Dataset<E> dataset, String uri) {
    return viewForUri(dataset, URI.create(uri));
  }

  /**
   * Convert a path to a partition directory in a filesystem dataset to a {@link View}
   * object representing that partition.
   * @param dataset the (partitioned) filesystem dataset
   * @param path the path to the partition directory
   * @return a view of the partition
   */
  public static <E> View<E> viewForPath(Dataset<E> dataset, Path path) {
    if (dataset instanceof FileSystemDataset) {
      return ((FileSystemDataset<E>) dataset).viewForUri(path.toUri());
    }
    throw new IllegalArgumentException("Not a file system dataset: " + dataset);
  }

}
