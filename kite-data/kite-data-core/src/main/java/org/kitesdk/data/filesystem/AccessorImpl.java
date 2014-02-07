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
package org.kitesdk.data.filesystem;

import java.io.IOException;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.PartitionKey;
import org.kitesdk.data.View;
import org.kitesdk.data.filesystem.impl.Accessor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

final class AccessorImpl extends Accessor {

  @Override
  public Path getDirectory(Dataset<?> dataset) {
    if (dataset instanceof FileSystemDataset) {
      return ((FileSystemDataset<?>) dataset).getDirectory();
    }
    return null;
  }

  @Override
  public Iterable<Path> getPathIterator(View view) {
    if (view instanceof FileSystemView) {
      return ((FileSystemView) view).pathIterator();
    } else if (view instanceof FileSystemDataset) {
      return ((FileSystemDataset) view).pathIterator();
    } else {
      throw new DatasetException(
          "Underlying Dataset must be a FileSystemDataset");
    }
  }

  @Override
  public void ensureExists(
      DatasetDescriptor descriptor, Configuration conf) {
    FileSystemDatasetRepository.ensureExists(descriptor, conf);
  }

  @Override
  public <E> PartitionKey fromDirectoryName(Dataset<E> dataset, Path dir) {
    if (dataset instanceof FileSystemDataset) {
      return ((FileSystemDataset<E>) dataset).fromDirectoryName(dir);
    }
    return null;
  }

  @Override
  public <E> void merge(Dataset<E> main, Dataset<E> update) throws IOException {
    if (main instanceof FileSystemDataset && update instanceof FileSystemDataset) {
      ((FileSystemDataset<E>) main).merge((FileSystemDataset<E>) update);
    }
  }
}
