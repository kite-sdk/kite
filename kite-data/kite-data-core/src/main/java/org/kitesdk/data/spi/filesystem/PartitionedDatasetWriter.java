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

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileSystem;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Flushable;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.Syncable;
import org.kitesdk.data.ValidationException;
import org.kitesdk.data.impl.Accessor;
import org.kitesdk.data.spi.AbstractDataset;
import org.kitesdk.data.spi.AbstractDatasetWriter;
import org.kitesdk.data.spi.DatasetRepositories;
import org.kitesdk.data.spi.DescriptorUtil;
import org.kitesdk.data.spi.EntityAccessor;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.Mergeable;
import org.kitesdk.data.spi.PartitionKey;
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
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.kitesdk.data.spi.TemporaryDatasetRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class PartitionedDatasetWriter<E, W extends FileSystemWriter<E>> extends
    AbstractDatasetWriter<E> {

  private static final Logger LOG = LoggerFactory
    .getLogger(PartitionedDatasetWriter.class);

  private static final int DEFAULT_WRITER_CACHE_SIZE = 10;

  protected FileSystemView<E> view;
  private final int maxWriters;

  private final PartitionStrategy partitionStrategy;
  protected LoadingCache<StorageKey, W> cachedWriters;

  private final StorageKey reusedKey;
  private final EntityAccessor<E> accessor;
  private final Map<String, Object> provided;

  protected ReaderWriterState state;

  static <E> PartitionedDatasetWriter<E, ?> newWriter(FileSystemView<E> view) {
    PartitionStrategy strategy = view.getDataset()
        .getDescriptor().getPartitionStrategy();

    // Use an immutable partition writer if immutable partitions are used,
    // otherwise just use a simple writer.
    if (Accessor.getDefault().immutablePartitionerCount(strategy) == 0) {
      return newSimpleWriter(view);

    } else {
      return newImmutableWriter(view);

    }
  }

  private static <E> PartitionedDatasetWriter<E, ?> newSimpleWriter(FileSystemView<E> view) {
    DatasetDescriptor descriptor = view.getDataset().getDescriptor();
    Format format = descriptor.getFormat();
    if (Formats.PARQUET.equals(format)) {
      // by default, Parquet is not durable
      if (DescriptorUtil.isDisabled(
          FileSystemProperties.NON_DURABLE_PARQUET_PROP, descriptor)) {
        return new IncrementalPartitionedDatasetWriter<E>(view);
      } else {
        return new NonDurablePartitionedDatasetWriter<E>(view);
      }
    } else if (Formats.AVRO.equals(format) || Formats.CSV.equals(format)) {
      return new IncrementalPartitionedDatasetWriter<E>(view);
    } else {
      return new NonDurablePartitionedDatasetWriter<E>(view);
    }
  }

  private static <E> PartitionedDatasetWriter<E, ?> newImmutableWriter(FileSystemView<E> view) {
    FileSystemDataset dataset = (FileSystemDataset) view.getDataset();

    // the target repository must be filesystem based because
    // the given view is.
    FileSystemDatasetRepository targetRepo =
        (FileSystemDatasetRepository) DatasetRepositories.repositoryFor(dataset.getUri());

    // create a temporary dataset containing a mutable view to write to,
    // which is then merged into the target when the write is complete
    // a UUID is used to ensure it is unique
    TemporaryDatasetRepository tempRepo = targetRepo.getTemporaryRepository(
        dataset.getNamespace(), UUID.randomUUID().toString().replace('-', '_'));

    DatasetDescriptor tempDescriptor = new DatasetDescriptor.Builder(
        dataset.getDescriptor()).location((URI) null).build();

    Dataset<E> tempDataset = tempRepo.create(dataset.getNamespace(),
        dataset.getName(), tempDescriptor);

    FileSystemView<E> tempView = (FileSystemView) ((AbstractDataset<E>) tempDataset)
        .filter(view.getConstraints());

    // create a writer for the temporary view and wrap it with one that
    // will do the atomic merge when closed.
    return new ImmutablePartitionedDatasetWriter<E>(tempView, view,
        newSimpleWriter(tempView), tempRepo);
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
      .build(createCacheLoader());

    state = ReaderWriterState.OPEN;
  }

  protected abstract CacheLoader<StorageKey, W> createCacheLoader();

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

  private void createEmptyViewPartition() {

    FileSystemDataset dataset = (FileSystemDataset) view.getDataset();

    // this assumes the query map preserves the order of the keys
    Collection<String> values = view.getConstraints().toQueryMap().values();
    PartitionKey key = new PartitionKey(values.toArray(new Object[values.size()]));

    Path partitionPath = dataset.toDirectoryName(dataset.getDirectory(), key);

    try {
      dataset.getFileSystem().mkdirs(partitionPath);

    } catch (FileAlreadyExistsException e) {

      // this is okay
    } catch (IOException e) {
      throw new DatasetIOException("Unable to create empty partition", e);
    }
  }

  @Override
  public void close() {
    if (state.equals(ReaderWriterState.OPEN)) {

      LOG.debug("Closing all cached writers for view:{}", view);

      for (DatasetWriter<E> writer : cachedWriters.asMap().values()) {
        LOG.debug("Closing partition writer:{}", writer);
        writer.close();
      }

      // even if no data was written to a specified sub-partition,
      // we still create the empty partition so readers can
      // tell it exists but has no data.
      if (cachedWriters.size() == 0 &&
          view.getConstraints().alignedWithBoundaries() &&
          view.getConstraints().toQueryMap().size() > 0) {

        createEmptyViewPartition();
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

  private static class IncrementalDatasetWriterCacheLoader<E> extends
      CacheLoader<StorageKey, FileSystemWriter.IncrementalWriter<E>> {

    private final FileSystemView<E> view;
    private final PathConversion convert;

    public IncrementalDatasetWriterCacheLoader(FileSystemView<E> view) {
      this.view = view;
      this.convert = new PathConversion(
          view.getDataset().getDescriptor().getSchema());
    }

    @Override
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
        value="BC_UNCONFIRMED_CAST_OF_RETURN_VALUE",
        justification="Writer is known to be IncrementalWriter")
    public FileSystemWriter.IncrementalWriter<E> load(StorageKey key) throws Exception {
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

      return (FileSystemWriter.IncrementalWriter<E>) writer;
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

  private static class NonDurablePartitionedDatasetWriter<E> extends
      PartitionedDatasetWriter<E, FileSystemWriter<E>> {

    private NonDurablePartitionedDatasetWriter(FileSystemView<E> view) {
      super(view);
    }

    @Override
    protected CacheLoader<StorageKey, FileSystemWriter<E>> createCacheLoader() {
      return new DatasetWriterCacheLoader<E>(view);
    }
  }

  private static class IncrementalPartitionedDatasetWriter<E> extends
      PartitionedDatasetWriter<E, FileSystemWriter.IncrementalWriter<E>>
      implements org.kitesdk.data.Flushable, Syncable {

    private IncrementalPartitionedDatasetWriter(FileSystemView<E> view) {
      super(view);
    }

    @Override
    protected CacheLoader<StorageKey, FileSystemWriter.IncrementalWriter<E>>
        createCacheLoader() {
      return new IncrementalDatasetWriterCacheLoader<E>(view);
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
      for (FileSystemWriter.IncrementalWriter<E> writer : cachedWriters.asMap().values()) {
        LOG.debug("Flushing partition writer:{}", writer);
        writer.flush();
      }
    }

    @Override
    public void sync() {
      Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
          "Attempt to sync a writer in state:%s", state);

      LOG.debug("Syncing all cached writers for view:{}", view);

      for (FileSystemWriter.IncrementalWriter<E> writer : cachedWriters.asMap().values()) {
        LOG.debug("Syncing partition writer:{}", writer);
        writer.sync();
      }
    }
  }

  /**
   * DatasetWriter that simply defers everything to a delegate writing to a temporary
   * location, and then merges into the final target when the writer is closed.
   */
  private static class ImmutablePartitionedDatasetWriter<E> extends PartitionedDatasetWriter<E, FileSystemWriter.IncrementalWriter<E>>
      implements org.kitesdk.data.Flushable, Syncable {

    private final PartitionedDatasetWriter<E, ?> delegate;

    private final FileSystemView<E> tempView;

    private final FileSystemView<E> targetView;

    private final TemporaryDatasetRepository tempRepo;

    private ImmutablePartitionedDatasetWriter(FileSystemView<E> tempView,
                                              FileSystemView<E> targetView,
                                              PartitionedDatasetWriter<E,?> delegate,
                                              TemporaryDatasetRepository tempRepo) {
      super(tempView);
      this.tempView = tempView;
      this.targetView = targetView;
      this.delegate = delegate;
      this.tempRepo = tempRepo;
    }

    @Override
    public void initialize() {
      delegate.initialize();
    }

    @Override
    public void write(E entity) {
      delegate.write(entity);
    }

    @Override
    public boolean isOpen() {
      return delegate.isOpen();
    }

    @Override
    public void flush() {
      if (delegate instanceof Flushable)
        ((Flushable) delegate).flush();
    }

    @Override
    protected CacheLoader<StorageKey, FileSystemWriter.IncrementalWriter<E>> createCacheLoader() {
      return (CacheLoader<StorageKey, FileSystemWriter.IncrementalWriter<E>>) delegate.createCacheLoader();
    }

    @Override
    public void sync() {

      if (delegate instanceof Syncable)
        ((Syncable) delegate).sync();
    }

    @Override
    public void close() {

      if (delegate.isOpen()) {

        delegate.close();

        // merge and delete the temporary data.
        ((Mergeable<Dataset<E>>) targetView.getDataset()).merge(tempView.getDataset());

        tempRepo.delete();

      }
    }
  }
}
