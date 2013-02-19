package com.cloudera.data;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class PartitionedDatasetWriter<E> implements DatasetWriter<E>, Closeable {

  private static final Logger logger = LoggerFactory
      .getLogger(PartitionedDatasetWriter.class);

  private final Dataset dataset;
  private int maxWriters;

  private final PartitionExpression expression;
  private LoadingCache<String, DatasetWriter<E>> cachedWriters;

  public PartitionedDatasetWriter(Dataset dataset) {
    Preconditions.checkArgument(dataset.isPartitioned(), "Dataset " + dataset
        + " is not partitioned");

    this.dataset = dataset;
    this.expression = dataset.getPartitionExpression();
    this.maxWriters = 10;
  }

  @Override
  public void open() {
    logger.debug("Opening partitioned dataset writer w/expression:{}",
        expression);

    /* TODO: Break out the inline anonymous classes. This is too hairy to read. */
    cachedWriters = CacheBuilder.newBuilder().maximumSize(maxWriters)
        .removalListener(new RemovalListener<String, DatasetWriter<E>>() {

          @Override
          public void onRemoval(
              RemovalNotification<String, DatasetWriter<E>> notification) {

            DatasetWriter<E> writer = notification.getValue();

            logger.debug("Removing writer:{} for partition:{}", writer,
                notification.getKey());

            try {
              writer.close();
            } catch (IOException e) {
              logger
                  .error(
                      "Failed to close the dataset writer:{} - {} (this may cause problems)",
                      writer, e.getMessage());
              logger.debug("Stack trace follows:", e);
            }
          }

        }).build(new CacheLoader<String, DatasetWriter<E>>() {

          @Override
          public DatasetWriter<E> load(String key) throws Exception {
            Partition<E> partition = dataset.getPartition(key, true);
            DatasetWriter<E> writer = partition.getWriter();

            writer.open();
            return writer;
          }

        });
  }

  @Override
  public void write(E entity) throws IOException {
    String name = expression.evaluate(entity);
    DatasetWriter<E> writer = null;

    try {
      writer = cachedWriters.get(name);
    } catch (ExecutionException e) {
      throw new IOException("Unable to get a writer for entity:" + entity
          + " partitionName:" + name, e);
    }

    writer.write(entity);
  }

  @Override
  public void flush() throws IOException {
    logger.debug("Flushing all cached writers");

    /*
     * There's a potential for flushing entries that are created by other
     * threads while looping through the writers. While normally just wasteful,
     * on HDFS, this is particularly bad. We should probably do something about
     * this, but it will be difficult as Cache (ideally) uses multiple
     * partitions to prevent cached writer contention.
     */
    for (DatasetWriter<E> writer : cachedWriters.asMap().values()) {
      writer.flush();
    }
  }

  @Override
  public void close() throws IOException {
    logger.debug("Closing all cached writers");

    for (DatasetWriter<E> writer : cachedWriters.asMap().values()) {
      writer.close();
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("expression", expression)
        .add("maxWriters", maxWriters).add("dataset", dataset)
        .add("cachedWriters", cachedWriters).toString();
  }

}
