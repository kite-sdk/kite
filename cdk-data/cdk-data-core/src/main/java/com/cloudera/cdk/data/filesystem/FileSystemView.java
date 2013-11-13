/*
 * Copyright 2013 Cloudera.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.cdk.data.filesystem;

import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.DatasetException;
import com.cloudera.cdk.data.DatasetReader;
import com.cloudera.cdk.data.DatasetWriter;
import com.cloudera.cdk.data.Formats;
import com.cloudera.cdk.data.View;
import com.cloudera.cdk.data.spi.AbstractRangeView;
import com.cloudera.cdk.data.spi.Key;
import com.cloudera.cdk.data.spi.MarkerRange;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;

import javax.annotation.concurrent.Immutable;
import java.io.IOException;

@Immutable
class FileSystemView<E> extends AbstractRangeView<E> {

  private final FileSystemDataset<E> fsDataset;

  FileSystemView(FileSystemDataset<E> dataset) {
    super(dataset);
    this.fsDataset = dataset;
  }

  private FileSystemView(FileSystemView<E> view, MarkerRange range) {
    super(view, range);
    this.fsDataset = view.fsDataset;
  }

  @Override
  protected FileSystemView<E> newLimitedCopy(MarkerRange newRange) {
    return new FileSystemView<E>(this, newRange);
  }

  @Override
  public DatasetReader<E> newReader() {
    final Iterable<Path> directories;
    if (dataset.getDescriptor().isPartitioned()) {
      directories = Iterables.transform(
          partitionIterator(),
          new Function<Key, Path>() {
            private final Path rootDirectory = fsDataset.getDirectory();
            private final PathConversion convert = new PathConversion();
            @Override
            public Path apply(Key key) {
              if (key != null) {
                return new Path(
                    rootDirectory, convert.fromKey(key));
              } else {
                throw new DatasetException("[BUG] Null partition");
              }
            }
          });
    } else {
      directories = Lists.newArrayList(fsDataset.getDirectory());
    }

    return new MultiFileDatasetReader<E>(
        fsDataset.getFileSystem(),
        new PathIterator(fsDataset.getFileSystem(), directories),
        dataset.getDescriptor());
  }

  @Override
  @SuppressWarnings("unchecked") // See https://github.com/Parquet/parquet-mr/issues/106
  public DatasetWriter<E> newWriter() {
    Preconditions.checkState(getDataset() instanceof FileSystemDataset,
        "FileSystemWriters cannot create writer for " + getDataset());

    final FileSystemDataset dataset = (FileSystemDataset) getDataset();

    if (dataset.getDescriptor().isPartitioned()) {
      return new PartitionedDatasetWriter<E>(this);
    } else {
      return FileSystemWriters.newFileWriter(
          dataset.getFileSystem(),
          dataset.getDirectory(),
          dataset.getDescriptor());
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterable<View<E>> getCoveringPartitions() {
    if (dataset.getDescriptor().isPartitioned()) {
      return Iterables.transform(
          partitionIterator(),
          new Function<Key, View<E>>() {
            @Override
            public View<E> apply(Key key) {
              if (key != null) {
                // no need for the bounds checks, use dataset.in
                return dataset.in(key);
              } else {
                throw new DatasetException("[BUG] Null partition");
              }
            }
          });
    } else {
      return Lists.newArrayList((View<E>) this);
    }
  }

  private FileSystemPartitionIterator partitionIterator() {
    try {
      return new FileSystemPartitionIterator(
          fsDataset.getFileSystem(), fsDataset.getDirectory(),
          dataset.getDescriptor().getPartitionStrategy(), range);
    } catch (IOException ex) {
      throw new DatasetException("Cannot list partitions in view:" + this, ex);
    }
  }
}
