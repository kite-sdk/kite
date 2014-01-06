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

import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.spi.StorageKey;
import org.kitesdk.data.spi.ReaderWriterState;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PartitionedDatasetWriter<E> implements DatasetWriter<E> {

  private static final Logger logger = LoggerFactory
    .getLogger(PartitionedDatasetWriter.class);

  private FileSystemView<E> view;
  private int maxWriters;

  private final PartitionStrategy partitionStrategy;
  private LoadingCache<StorageKey, DatasetWriter<E>> cachedWriters;

  private final StorageKey reusedKey;

  private ReaderWriterState state;

  public PartitionedDatasetWriter(FileSystemView<E> view) {
    final DatasetDescriptor descriptor = view.getDataset().getDescriptor();
    Preconditions.checkArgument(descriptor.isPartitioned(),
        "Dataset " + view.getDataset() + " is not partitioned");

    this.view = view;
    this.partitionStrategy = descriptor.getPartitionStrategy();
    this.maxWriters = Math.min(10, partitionStrategy.getCardinality());
    this.state = ReaderWriterState.NEW;
    this.reusedKey = new StorageKey(partitionStrategy);
  }

  @Override
  public void open() {
    Preconditions.checkState(state.equals(ReaderWriterState.NEW),
      "Unable to open a writer from state:%s", state);

    logger.debug("Opening partitioned dataset writer w/strategy:{}",
      partitionStrategy);

    cachedWriters = CacheBuilder.newBuilder().maximumSize(maxWriters)
      .removalListener(new DatasetWriterCloser<E>())
      .build(new DatasetWriterCacheLoader<E>(view));

    state = ReaderWriterState.OPEN;
  }

  @Override
  public void write(E entity) {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to write to a writer in state:%s", state);

    reusedKey.reuseFor(entity);

    DatasetWriter<E> writer = cachedWriters.getIfPresent(reusedKey);
    if (writer == null) {
      // get a new key because it is stored in the cache
      StorageKey key = StorageKey.copy(reusedKey);
      try {
        writer = cachedWriters.getUnchecked(key);
      } catch (UncheckedExecutionException ex) {
        // catch & release: the correct exception is that the entity cannot be
        // written because it isn't in the View. But to avoid checking in every
        // write call, check when writers are created, catch the exception, and
        // throw the correct one here.
        throw new IllegalArgumentException(
            "View does not contain entity:" + entity, ex.getCause());
      }
    }

    writer.write(entity);
  }

  @Override
  public void flush() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
      "Attempt to write to a writer in state:%s", state);

    logger.debug("Flushing all cached writers for view:{}", view);

    /*
     * There's a potential for flushing entries that are created by other
     * threads while looping through the writers. While normally just wasteful,
     * on HDFS, this is particularly bad. We should probably do something about
     * this, but it will be difficult as Cache (ideally) uses multiple
     * partitions to prevent cached writer contention.
     */
    for (DatasetWriter<E> writer : cachedWriters.asMap().values()) {
      logger.debug("Flushing partition writer:{}", writer);
      writer.flush();
    }
  }

  @Override
  public void close() {
    if (state.equals(ReaderWriterState.OPEN)) {

      logger.debug("Closing all cached writers for view:{}", view);

      for (DatasetWriter<E> writer : cachedWriters.asMap().values()) {
        logger.debug("Closing partition writer:{}", writer);
        writer.close();
      }

      state = ReaderWriterState.CLOSED;
    }
  }

  @Override
  public boolean isOpen() {
    return state.equals(ReaderWriterState.OPEN);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("partitionStrategy", partitionStrategy)
        .add("maxWriters", maxWriters)
        .add("view", view)
        .add("cachedWriters", cachedWriters)
        .toString();
  }

  private static class DatasetWriterCacheLoader<E> extends
    CacheLoader<StorageKey, DatasetWriter<E>> {

    private final FileSystemView<E> view;
    private final PathConversion convert;

    public DatasetWriterCacheLoader(FileSystemView<E> view) {
      this.view = view;
      this.convert = new PathConversion();
    }

    @Override
    public DatasetWriter<E> load(StorageKey key) throws Exception {
      Preconditions.checkArgument(view.contains(key),
          "View {} does not contain StorageKey {}", view, key);
      Preconditions.checkState(view.getDataset() instanceof FileSystemDataset,
          "FileSystemWriters cannot create writer for " + view.getDataset());

      final FileSystemDataset dataset = (FileSystemDataset) view.getDataset();
      final DatasetWriter<E> writer = FileSystemWriters.newFileWriter(
          dataset.getFileSystem(),
          new Path(dataset.getDirectory(), convert.fromKey(key)),
          dataset.getDescriptor(), dataset.getPartitionListener(), dataset.getName(), key);

      writer.open();

      return writer;
    }

  }

  private static class DatasetWriterCloser<E> implements
    RemovalListener<StorageKey, DatasetWriter<E>> {

    @Override
    public void onRemoval(
      RemovalNotification<StorageKey, DatasetWriter<E>> notification) {

      DatasetWriter<E> writer = notification.getValue();

      logger.debug("Closing writer:{} for partition:{}", writer,
        notification.getKey());

      writer.close();
    }

  }
}
