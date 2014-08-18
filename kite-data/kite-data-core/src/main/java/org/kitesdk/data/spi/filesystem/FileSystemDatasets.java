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

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import java.net.URI;
import java.util.Iterator;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.RefinableView;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.Conversions;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.SchemaUtil;

/**
 * <p>
 * A helper class working with {@link org.kitesdk.data.spi.filesystem.FileSystemDataset}s.
 * </p>
 */
public class FileSystemDatasets {

  private static final Splitter PATH_SPLITTER = Splitter.on('/');
  private static final Splitter KV_SPLITTER = Splitter.on('=').limit(2);

  /**
   * Convert a URI for a partition directory in a filesystem dataset to a {@link View}
   * object representing that partition.
   * @param dataset the (partitioned) filesystem dataset
   * @param uri the path to the partition directory
   * @return a view of the partition
   */
  public static <E> View<E> viewForUri(Dataset<E> dataset, URI uri) {
    Preconditions.checkArgument(dataset instanceof FileSystemDataset,
        "Not a file system dataset: " + dataset);

    DatasetDescriptor descriptor = dataset.getDescriptor();

    String s1 = descriptor.getLocation().getScheme();
    String s2 = uri.getScheme();
    Preconditions.checkArgument((s1 == null || s2 == null) || s1.equals(s2),
        "%s is not contained in %s", uri, descriptor.getLocation());

    URI location = URI.create(descriptor.getLocation().getPath());
    URI relative = location.relativize(URI.create(uri.getPath()));
    if (relative.toString().isEmpty()) {
      // no partitions are selected
      return dataset;
    }

    Preconditions.checkArgument(!relative.getPath().startsWith("/"),
        "%s is not contained in %s", uri, location);
    Preconditions.checkArgument(descriptor.isPartitioned(),
        "Dataset is not partitioned");

    Schema schema = descriptor.getSchema();
    PartitionStrategy strategy = descriptor.getPartitionStrategy();

    RefinableView<E> view = dataset;
    Iterator<String> parts = PATH_SPLITTER.split(relative.toString()).iterator();
    for (FieldPartitioner fp : strategy.getFieldPartitioners()) {
      if (!parts.hasNext()) {
        break;
      }
      String value = Iterables.getLast(KV_SPLITTER.split(parts.next()));
      Schema fieldSchema = SchemaUtil.fieldSchema(schema, strategy, fp.getName());
      view = view.with(fp.getName(), Conversions.convert(value, fieldSchema));
    }
    return view;
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
    return viewForUri(dataset, path.toUri());
  }

}
