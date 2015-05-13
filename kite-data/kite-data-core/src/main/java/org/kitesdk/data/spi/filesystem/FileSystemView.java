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
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Signalable;
import org.kitesdk.data.spi.AbstractDatasetReader;
import org.kitesdk.data.spi.AbstractDatasetWriter;
import org.kitesdk.data.spi.AbstractRefinableView;
import org.kitesdk.data.spi.Constraints;
import org.kitesdk.data.spi.InputFormatAccessor;
import org.kitesdk.data.spi.LastModifiedAccessor;
import org.kitesdk.data.spi.Pair;
import org.kitesdk.data.spi.PartitionListener;
import org.kitesdk.data.spi.SizeAccessor;
import org.kitesdk.data.spi.StorageKey;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.kitesdk.data.RefinableView;
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
    LastModifiedAccessor, SizeAccessor, Signalable<E> {

  private static final Logger LOG = LoggerFactory.getLogger(FileSystemView.class);

  private final FileSystem fs;
  private final Path root;

  private final PartitionListener listener;

  private final SignalManager signalManager;

  FileSystemView(FileSystemDataset<E> dataset, @Nullable PartitionListener listener, @Nullable SignalManager signalManager, Class<E> type) {
    super(dataset, type);
    this.fs = dataset.getFileSystem();
    this.root = dataset.getDirectory();
    this.listener = listener;
    this.signalManager = signalManager;
  }

  private FileSystemView(FileSystemView<E> view, Constraints c) {
    super(view, c);
    this.fs = view.fs;
    this.root = view.root;
    this.listener = view.listener;
    this.signalManager = view.signalManager;
  }

  private FileSystemView(FileSystemView<?> view, Schema schema) {
    super(view, schema);
    this.fs = view.fs;
    this.root = view.root;
    this.listener = view.listener;
  }

  @Override
  protected FileSystemView<E> filter(Constraints c) {
    return new FileSystemView<E>(this, c);
  }

  @Override
  public <T extends GenericRecord> RefinableView<T> asSchema(Schema schema) {
    return new FileSystemView<T>(this, schema);
  }

  @Override
  public DatasetReader<E> newReader() {
    AbstractDatasetReader<E> reader = new MultiFileDatasetReader<E>(fs,
        pathIterator(), dataset.getDescriptor(), constraints, getAccessor());
    reader.initialize();
    return reader;
  }

  @Override
  public DatasetWriter<E> newWriter() {
    AbstractDatasetWriter<E> writer;
    if (dataset.getDescriptor().isPartitioned()) {
      writer = PartitionedDatasetWriter.newWriter(this);
    } else {
      writer = FileSystemWriter.newWriter(fs, root, dataset.getDescriptor());
    }
    writer.initialize();
    return writer;
  }

  @Override
  public boolean deleteAll() {
    if (!constraints.alignedWithBoundaries()) {
      throw new UnsupportedOperationException(
          "Cannot cleanly delete view: " + this);
    }
    return deleteAllUnsafe();
  }

  @Override
  public InputFormat<E, Void> getInputFormat(Configuration conf) {
    return new FileSystemViewKeyInputFormat<E>(this, conf);
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
    DatasetDescriptor descriptor = dataset.getDescriptor();
    try {
      return new FileSystemPartitionIterator(
          fs, root, descriptor.getPartitionStrategy(), descriptor.getSchema(),
          constraints);
    } catch (IOException ex) {
      throw new DatasetException("Cannot list partitions in view:" + this, ex);
    }
  }

  boolean deleteAllUnsafe() {
    boolean deleted = false;
    if (dataset.getDescriptor().isPartitioned()) {
      for (Pair<StorageKey, Path> partition : partitionIterator()) {
        deleted = FileSystemUtil.cleanlyDelete(fs, root, partition.second()) || deleted;

        if (listener != null) {

          // the relative path is the partition name, so we can simply delete it
          // in Hive
          listener.partitionDeleted(dataset.getNamespace(),
              dataset.getName(), partition.second().toString());
        }

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

    // if view was marked ready more recently count it as the modified time
    if (signalManager != null) {
      long readyTimestamp = signalManager.getReadyTimestamp(getConstraints());
      if (lastMod < readyTimestamp) {
        lastMod = readyTimestamp;
      }
    }

    return lastMod;
  }

  @Override
  public void signalReady() {
    if (signalManager != null) {
      signalManager.signalReady(getConstraints());
    }
  }

  @Override
  public boolean isReady() {
    if (signalManager != null) {
      long readyTimestamp = signalManager.getReadyTimestamp(getConstraints());
      return readyTimestamp != -1;
    }
    return false;
  }
}
