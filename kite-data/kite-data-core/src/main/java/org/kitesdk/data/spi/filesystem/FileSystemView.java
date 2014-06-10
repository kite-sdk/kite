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

package org.kitesdk.data.spi.filesystem;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.InputFormat;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.NotReadySignalableException;
import org.kitesdk.data.PartitionKey;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.spi.AbstractRefinableView;
import org.kitesdk.data.spi.Constraints;
import org.kitesdk.data.spi.InputFormatAccessor;
import org.kitesdk.data.spi.LastModifiedAccessor;
import org.kitesdk.data.spi.Pair;
import org.kitesdk.data.spi.ReadySignalable;
import org.kitesdk.data.spi.SizeAccessor;
import org.kitesdk.data.spi.StorageKey;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileSystem implementation of a {@link org.kitesdk.data.spi.Constraints}-based
 * {@link org.kitesdk.data.RefinableView}.
 *
 * @param <E> The type of records read and written by this view.
 */
@Immutable
class FileSystemView<E> extends AbstractRefinableView<E> implements InputFormatAccessor<E>,
    LastModifiedAccessor, SizeAccessor, ReadySignalable {

  private static final String READY_SIGNAL_FILENAME = "_SUCCESS";

  private static final Logger LOG = LoggerFactory.getLogger(FileSystemView.class);

  private final FileSystem fs;
  private final Path root;

  FileSystemView(FileSystemDataset<E> dataset) {
    super(dataset);
    this.fs = dataset.getFileSystem();
    this.root = dataset.getDirectory();
  }

  private FileSystemView(FileSystemView<E> view, Constraints c) {
    super(view, c);
    this.fs = view.fs;
    this.root = view.root;
  }

  @Override
  protected FileSystemView<E> filter(Constraints c) {
    return new FileSystemView<E>(this, c);
  }

  @Override
  public DatasetReader<E> newReader() {
    return new MultiFileDatasetReader<E>(
        fs, pathIterator(), dataset.getDescriptor(), constraints);
  }

  @Override
  public DatasetWriter<E> newWriter() {
    if (dataset.getDescriptor().isPartitioned()) {
      return new PartitionedDatasetWriter<E>(this);
    } else {
      return new FileSystemWriter<E>(fs, root, dataset.getDescriptor());
    }
  }

  @Override
  public boolean deleteAll() {
    DatasetDescriptor descriptor = getDataset().getDescriptor();
    if (!descriptor.isPartitioned()) {
      // at least one constraint, but not partitioning to satisfy it
      throw new UnsupportedOperationException(
          "Cannot cleanly delete view: " + this);
    }
    if (!constraints.alignedWithBoundaries(descriptor.getPartitionStrategy())) {
      throw new UnsupportedOperationException(
          "Cannot cleanly delete view: " + this);
    }
    return deleteAllUnsafe();
  }

  @Override
  public InputFormat<E, Void> getInputFormat() {
    return new FileSystemViewKeyInputFormat<E>(this);
  }

  PathIterator pathIterator() {
    Iterator<Pair<StorageKey, Path>> directories;
    if (dataset.getDescriptor().isPartitioned()) {
      directories = partitionIterator();
    } else {
      directories = Iterators.singletonIterator(
          Pair.of((StorageKey) null, root));
    }
    return new PathIterator(fs, root, directories);
  }

  /**
   * Returns an iterator that provides all leaf-level directories in this view.
   *
   * @return leaf-directory iterator
   */
  Iterator<Path> dirIterator() {
    if (dataset.getDescriptor().isPartitioned()) {
      return Iterators.transform(partitionIterator(), new Function<Pair<StorageKey, Path>, Path>() {
        @Override
        @edu.umd.cs.findbugs.annotations.SuppressWarnings(
            value="NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE",
            justification="False positive, initialized above as non-null.")
        public Path apply(@Nullable Pair<StorageKey, Path> input) {
          return new Path(root, input.second());
        }
      });
    } else {
      return Iterators.singletonIterator(root);
    }
  }

  private FileSystemPartitionIterator partitionIterator() {
    try {
      return new FileSystemPartitionIterator(
          fs, root,
          dataset.getDescriptor().getPartitionStrategy(), constraints);
    } catch (IOException ex) {
      throw new DatasetException("Cannot list partitions in view:" + this, ex);
    }
  }

  boolean deleteAllUnsafe() {
    boolean deleted = false;
    if (dataset.getDescriptor().isPartitioned()) {
      for (Pair<StorageKey, Path> partition : partitionIterator()) {
        deleted = FileSystemUtil.cleanlyDelete(fs, root, partition.second()) || deleted;
      }
    }
    else {
      for (Path path : pathIterator()) {
        deleted = FileSystemUtil.cleanlyDelete(fs, root, path) || deleted;
      }
    }
    return deleted;
  }

  @Override
  public long getSize() {
    long size = 0;
    for (Iterator<Path> i = dirIterator(); i.hasNext(); ) {
      Path dir = i.next();
      try {
        for (FileStatus st : fs.listStatus(dir)) {
          size += st.getLen();
        }
      } catch (IOException e) {
        throw new DatasetIOException("Cannot find size of " + dir, e);
      }
    }
    return size;
  }

  @Override
  public long getLastModified() {
    long lastMod = -1;
    for (Iterator<Path> i = dirIterator(); i.hasNext(); ) {
      Path dir = i.next();
      try {
        for (FileStatus st : fs.listStatus(dir)) {
          if (lastMod < st.getModificationTime()) {
            lastMod = st.getModificationTime();
          }
        }
      } catch (IOException e) {
        throw new DatasetIOException("Cannot find last modified time of of " + dir, e);
      }
    }
    return lastMod;
  }
  
  @Override
  public boolean isReady() {
    if (isReady(fs, root, new Path("."))) {
      // whole dataset is ready
      return true;
    }
    
    if (constraints.isUnbounded()) {
      // not ready unless the whole dataset is ready
      return false;
    }
    
    if (dataset.getDescriptor().isPartitioned()) {
      DatasetDescriptor descriptor = dataset.getDescriptor();
      PartitionStrategy partitionStrategy = descriptor.getPartitionStrategy();
      if (!constraints.convertableToPartitionKeys(partitionStrategy)) {
        throw new NotReadySignalableException("Cannot cleanly signal view: "
            + this);
      }
      int readyPartitions = 0;
      int totalPartitions = 0;
      for (PartitionKey key : constraints.toPartitionKeys(partitionStrategy)) {
        Dataset<E> partition = dataset.getPartition(key, false);
        totalPartitions++;
        if (partition != null
            && isReady(fs, root, new Path(partition.getDescriptor()
                .getLocation()))) {
          readyPartitions++;
        }
      }
      return readyPartitions > 0 && readyPartitions == totalPartitions;
    }
    
    // at least one constraint, but not partitioning to satisfy it
    throw new NotReadySignalableException("Cannot cleanly signal view: "
        + this);
  }

  @Override
  public void signalReady() {
    if (constraints.isUnbounded()) {
      signalReady(fs, root, new Path("."));
      return;
    }
    
    DatasetDescriptor descriptor = getDataset().getDescriptor();
    if (!descriptor.isPartitioned()) {
      // at least one constraint, but not partitioning to satisfy it
      return;
    }
    PartitionStrategy partitionStrategy = descriptor.getPartitionStrategy();
    if (!constraints.convertableToPartitionKeys(partitionStrategy)) {
      return;
    }

    // make sure the partitions to be signaled exist
    for (PartitionKey key : constraints.toPartitionKeys(partitionStrategy)) {
      dataset.getPartition(key, true);
    }
    
    for (Pair<StorageKey, Path> partition : partitionIterator()) {
      signalReady(fs, root, partition.second());
    }
  }

  private static void signalReady(FileSystem fs, Path root, Path dir) {
    try {
      if (dir.isAbsolute()) {
        LOG.debug("Creating ready signal file in {}", dir);
        fs.create(new Path(dir, READY_SIGNAL_FILENAME));
      } else {
        // the path should be treated as relative to the root path
        Path absolute = new Path(root, dir);
        LOG.debug("Creating ready signal file in {}", absolute);
        fs.create(new Path(absolute, READY_SIGNAL_FILENAME));
      }
    } catch (IOException ex) {
      throw new DatasetIOException("Could not create ready signal file:" + dir, ex);
    }
  }
  
  private static boolean isReady(FileSystem fs, Path root, Path dir) {
    try {
      if (dir.isAbsolute()) {
        LOG.debug("Checking ready signal file in {}", dir);
        return fs.exists(new Path(dir, READY_SIGNAL_FILENAME));
      } else {
        // the path should be treated as relative to the root path
        Path absolute = new Path(root, dir);
        LOG.debug("Checking ready signal file in {}", absolute);
        return fs.exists(new Path(absolute, READY_SIGNAL_FILENAME));
      }
    } catch (IOException ex) {
      throw new DatasetIOException("Could not check ready signal file:" + dir, ex);
    }
  }
}
