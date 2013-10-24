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
package com.cloudera.cdk.data.filesystem;

import com.cloudera.cdk.data.Dataset;
import com.cloudera.cdk.data.DatasetWriter;
import com.cloudera.cdk.data.DatasetWriterException;
import com.cloudera.cdk.data.PartitionKey;
import com.cloudera.cdk.data.PartitionStrategy;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutionException;

class PartitionedDatasetWriter<E> implements DatasetWriter<E> {

  private static final Logger logger = LoggerFactory
    .getLogger(PartitionedDatasetWriter.class);

  private Dataset dataset;
  private int maxWriters;

  private final PartitionStrategy partitionStrategy;
  private LoadingCache<PartitionKey, DatasetWriter<E>> cachedWriters;
  private PartitionKey key;

  private ReaderWriterState state;

  public PartitionedDatasetWriter(Dataset dataset) {
    Preconditions.checkArgument(dataset.getDescriptor().isPartitioned(),
      "Dataset " + dataset + " is not partitioned");

    this.dataset = dataset;
    this.partitionStrategy = dataset.getDescriptor().getPartitionStrategy();
    this.maxWriters = Math.min(10, partitionStrategy.getCardinality());
    this.state = ReaderWriterState.NEW;
  }

  @Deprecated
  public PartitionedDatasetWriter(Dataset dataset,
    PartitionStrategy partitionStrategy) {

    Preconditions.checkArgument(dataset.getDescriptor().isPartitioned(),
      "Dataset " + dataset + " is not partitioned");
    Preconditions
      .checkArgument(
        dataset.getDescriptor().getPartitionStrategy()
          .equals(partitionStrategy),
        "Dataset descriptor's partitions strategy doesn't match the provided partition strategy");

    this.dataset = dataset;
    this.partitionStrategy = dataset.getDescriptor().getPartitionStrategy();
    this.maxWriters = Math.min(10, this.partitionStrategy.getCardinality());
    this.state = ReaderWriterState.NEW;
  }

  @Override
  public void open() {
    Preconditions.checkState(state.equals(ReaderWriterState.NEW),
      "Unable to open a writer from state:%s", state);

    logger.debug("Opening partitioned dataset writer w/strategy:{}",
      partitionStrategy);

    cachedWriters = CacheBuilder.newBuilder().maximumSize(maxWriters)
      .removalListener(new DatasetWriterRemovalStrategy<E>())
      .build(new DatasetWriterCacheLoader<E>(dataset));

    state = ReaderWriterState.OPEN;
  }

  @Override
  @SuppressWarnings("deprecation")
  public void write(E entity) {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
      "Attempt to write to a writer in state:%s", state);

    key = partitionStrategy.partitionKeyForEntity(entity, key);
    DatasetWriter<E> writer;

    try {
      writer = cachedWriters.get(key);
    } catch (ExecutionException e) {
      throw new DatasetWriterException("Unable to get a writer for entity:" + entity
        + " partition key:" + Arrays.asList(key), e);
    }

    writer.write(entity);
  }

  @Override
  public void flush() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
      "Attempt to write to a writer in state:%s", state);

    logger.debug("Flushing all cached writers for partition strategy:{}",
      partitionStrategy);

    /*
     * There's a potential for flushing entries that are created by other
     * threads while looping through the writers. While normally just wasteful,
     * on HDFS, this is particularly bad. We should probably do something about
     * this, but it will be difficult as Cache (ideally) uses multiple
     * partitions to prevent cached writer contention.
     */
    for (Map.Entry<PartitionKey, DatasetWriter<E>> entry : cachedWriters
      .asMap().entrySet()) {
      logger.debug("Flushing partition writer:{}.{}", entry.getKey(),
        entry.getValue());
      entry.getValue().flush();
    }
  }

  @Override
  public void close() {
    if (state.equals(ReaderWriterState.OPEN)) {

      logger.debug("Closing all cached writers for partition strategy:{}",
        partitionStrategy);

      for (Map.Entry<PartitionKey, DatasetWriter<E>> entry : cachedWriters
        .asMap().entrySet()) {
        logger.debug("Closing partition writer:{}.{}", entry.getKey(),
          entry.getValue());
        entry.getValue().close();
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
      .add("maxWriters", maxWriters).add("dataset", dataset)
      .add("cachedWriters", cachedWriters).toString();
  }

  private static class DatasetWriterCacheLoader<E> extends
    CacheLoader<PartitionKey, DatasetWriter<E>> {

    private Dataset dataset;

    public DatasetWriterCacheLoader(Dataset dataset) {
      this.dataset = dataset;
    }

    @Override
    public DatasetWriter<E> load(PartitionKey key) throws Exception {
      Dataset partition = dataset.getPartition(key, true);
      DatasetWriter<E> writer = partition.newWriter();

      writer.open();
      return writer;
    }

  }

  private static class DatasetWriterRemovalStrategy<E> implements
    RemovalListener<PartitionKey, DatasetWriter<E>> {

    @Override
    public void onRemoval(
      RemovalNotification<PartitionKey, DatasetWriter<E>> notification) {

      DatasetWriter<E> writer = notification.getValue();

      logger.debug("Removing writer:{} for partition:{}", writer,
        notification.getKey());

      writer.close();
    }

  }

}
