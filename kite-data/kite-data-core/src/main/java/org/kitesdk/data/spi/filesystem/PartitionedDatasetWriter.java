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
package org.kitesdk.data.spi.filesystem;

import java.util.Map;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Flushable;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.Syncable;
import org.kitesdk.data.ValidationException;
import org.kitesdk.data.spi.AbstractDatasetWriter;
import org.kitesdk.data.spi.DescriptorUtil;
import org.kitesdk.data.spi.EntityAccessor;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.PartitionListener;
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

class PartitionedDatasetWriter<E> extends AbstractDatasetWriter<E> {

  private static final Logger LOG = LoggerFactory
    .getLogger(PartitionedDatasetWriter.class);

  private static final int DEFAULT_WRITER_CACHE_SIZE = 10;

  protected FileSystemView<E> view;
  private final int maxWriters;

  private final PartitionStrategy partitionStrategy;
  protected LoadingCache<StorageKey, FileSystemWriter<E>> cachedWriters;

  private final StorageKey reusedKey;
  private final EntityAccessor<E> accessor;
  private final Map<String, Object> provided;

  protected ReaderWriterState state;

  static <E> PartitionedDatasetWriter<E> newWriter(FileSystemView<E> view) {
    DatasetDescriptor descriptor = view.getDataset().getDescriptor();
    Format format = descriptor.getFormat();
    if (Formats.PARQUET.equals(format)) {
      // by default, Parquet is not durable
      if (DescriptorUtil.isDisabled(
          FileSystemProperties.NON_DURABLE_PARQUET_PROP, descriptor)) {
        return new IncrementalPartitionedDatasetWriter<E>(view);
      } else {
        return new PartitionedDatasetWriter<E>(view);
      }
    } else if (Formats.AVRO.equals(format) || Formats.CSV.equals(format)) {
      return new IncrementalPartitionedDatasetWriter<E>(view);
    } else {
      return new PartitionedDatasetWriter<E>(view);
    }
  }

  private PartitionedDatasetWriter(FileSystemView<E> view) {
    final DatasetDescriptor descriptor = view.getDataset().getDescriptor();
    Preconditions.checkArgument(descriptor.isPartitioned(),
        "Dataset " + view.getDataset() + " is not partitioned");

    this.view = view;
    this.partitionStrategy = descriptor.getPartitionStrategy();

    int maxWriters = DEFAULT_WRITER_CACHE_SIZE;
    if (descriptor.hasProperty(FileSystemProperties.WRITER_CACHE_SIZE_PROP)) {
      try {
        maxWriters = Integer.parseInt(
            descriptor.getProperty(FileSystemProperties.WRITER_CACHE_SIZE_PROP));
      } catch (NumberFormatException e) {
        LOG.warn("Not an integer: " + FileSystemProperties.WRITER_CACHE_SIZE_PROP + "=" +
            descriptor.getProperty(FileSystemProperties.WRITER_CACHE_SIZE_PROP));
      }
    } else if (partitionStrategy.getCardinality() != FieldPartitioner.UNKNOWN_CARDINALITY) {
        maxWriters = Math.min(maxWriters, partitionStrategy.getCardinality());
    }
    this.maxWriters = maxWriters;

    this.state = ReaderWriterState.NEW;
    this.reusedKey = new StorageKey(partitionStrategy);
    this.accessor = view.getAccessor();
    this.provided = view.getProvidedValues();
  }

  @Override
  public void initialize() {
    Preconditions.checkState(state.equals(ReaderWriterState.NEW),
        "Unable to open a writer from state:%s", state);

    DatasetDescriptor descriptor = view.getDataset().getDescriptor();
    ValidationException.check(
        FileSystemWriter.isSupportedFormat(descriptor),
        "Not a supported format: %s", descriptor.getFormat());

    LOG.debug("Opening partitioned dataset writer w/strategy:{}",
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

    accessor.keyFor(entity, provided, reusedKey);

    DatasetWriter<E> writer = cachedWriters.getIfPresent(reusedKey);
    if (writer == null) {
      // avoid checking in every whether the entity belongs in the view by only
      // checking when a new writer is created
      Preconditions.checkArgument(view.includes(entity),
          "View %s does not include entity %s", view, entity);
      // get a new key because it is stored in the cache
      StorageKey key = StorageKey.copy(reusedKey);
      try {
        writer = cachedWriters.getUnchecked(key);
      } catch (UncheckedExecutionException ex) {
        throw new IllegalArgumentException(
            "Problem creating view for entity: " + entity, ex.getCause());
      }
    }

    writer.write(entity);
  }

  @Override
  public void close() {
    if (state.equals(ReaderWriterState.OPEN)) {

      LOG.debug("Closing all cached writers for view:{}", view);

      for (DatasetWriter<E> writer : cachedWriters.asMap().values()) {
        LOG.debug("Closing partition writer:{}", writer);
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
    CacheLoader<StorageKey, FileSystemWriter<E>> {

    private final FileSystemView<E> view;
    private final PathConversion convert;

    public DatasetWriterCacheLoader(FileSystemView<E> view) {
      this.view = view;
      this.convert = new PathConversion(
          view.getDataset().getDescriptor().getSchema());
    }

    @Override
    public FileSystemWriter<E> load(StorageKey key) throws Exception {
      Preconditions.checkState(view.getDataset() instanceof FileSystemDataset,
          "FileSystemWriters cannot create writer for " + view.getDataset());

      FileSystemDataset dataset = (FileSystemDataset) view.getDataset();
      Path partition = convert.fromKey(key);
      FileSystemWriter<E> writer = FileSystemWriter.newWriter(
          dataset.getFileSystem(),
          new Path(dataset.getDirectory(), partition),
          dataset.getDescriptor());

      PartitionListener listener = dataset.getPartitionListener();
      if (listener != null) {
        listener.partitionAdded(
            dataset.getNamespace(), dataset.getName(), partition.toString());
      }

      writer.initialize();

      return writer;
    }

  }

  private static class DatasetWriterCloser<E> implements
    RemovalListener<StorageKey, DatasetWriter<E>> {

    @Override
    public void onRemoval(
      RemovalNotification<StorageKey, DatasetWriter<E>> notification) {

      DatasetWriter<E> writer = notification.getValue();

      LOG.debug("Closing writer:{} for partition:{}", writer,
        notification.getKey());

      writer.close();
    }

  }

  private static class IncrementalPartitionedDatasetWriter<E> extends
      PartitionedDatasetWriter<E> implements org.kitesdk.data.Flushable, Syncable {

    private IncrementalPartitionedDatasetWriter(FileSystemView<E> view) {
      super(view);
    }

    @Override
    public void flush() {
      Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
          "Attempt to flush a writer in state:%s", state);

      LOG.debug("Flushing all cached writers for view:{}", view);

    /*
     * There's a potential for flushing entries that are created by other
     * threads while looping through the writers. While normally just wasteful,
     * on HDFS, this is particularly bad. We should probably do something about
     * this, but it will be difficult as Cache (ideally) uses multiple
     * partitions to prevent cached writer contention.
     */
      for (FileSystemWriter<E> writer : cachedWriters.asMap().values()) {
        LOG.debug("Flushing partition writer:{}", writer);
        if (writer instanceof Flushable) {
          ((Flushable) writer).flush();
        }
      }
    }

    @Override
    public void sync() {
      Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
          "Attempt to sync a writer in state:%s", state);

      LOG.debug("Syncing all cached writers for view:{}", view);

      for (FileSystemWriter<E> writer : cachedWriters.asMap().values()) {
        LOG.debug("Syncing partition writer:{}", writer);
        if (writer instanceof Syncable) {
          ((Syncable) writer).sync();
        }
      }
    }
  }
}
