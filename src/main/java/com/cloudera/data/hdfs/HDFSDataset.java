package com.cloudera.data.hdfs;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.data.Dataset;
import com.cloudera.data.DatasetReader;
import com.cloudera.data.DatasetWriter;
import com.cloudera.data.PartitionExpression;
import com.cloudera.data.PartitionedDatasetWriter;
import com.cloudera.data.partition.PartitionStrategy;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;

public class HDFSDataset implements Dataset {

  private static final Logger logger = LoggerFactory
      .getLogger(HDFSDataset.class);

  private FileSystem fileSystem;
  private Path directory;
  private Path dataDirectory;
  private String name;
  private Schema schema;
  private PartitionStrategy partitionStrategy;

  @Override
  public <E> DatasetWriter<E> getWriter() {
    DatasetWriter<E> writer = null;

    if (isPartitioned()) {
      // FIXME: Why does this complain about a resource leak and not others?
      writer = new PartitionedDatasetWriter<E>(this);
    } else {
      // FIXME: This file name is not guaranteed to be truly unique.
      Path dataFile = new Path(dataDirectory, Joiner.on('-').join(
          System.currentTimeMillis(), Thread.currentThread().getId()));

      writer = new HDFSDatasetWriter.Builder<E>().fileSystem(fileSystem)
          .path(dataFile).schema(schema).get();
    }

    return writer;
  }

  @Override
  public <E> DatasetReader<E> getReader() {
    throw new UnsupportedOperationException(
        "Attempt to get a reader for dataset:" + name);
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  @Override
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Override
  public PartitionExpression getPartitionExpression() {
    throw new UnsupportedOperationException("Use getPartitionStrategy()!");
  }

  @Override
  public PartitionStrategy getPartitionStrategy() {
    return partitionStrategy;
  }

  @Override
  public boolean isPartitioned() {
    return partitionStrategy != null;
  }

  @Override
  public Dataset getPartition(String name, boolean allowCreate)
      throws IOException {

    Preconditions.checkState(isPartitioned(),
        "Attempt to get a partition on a non-partitioned dataset (name:%s)",
        name);

    logger.debug("Loading partition name:{} allowCreate:{}", name, allowCreate);

    Path partitionDirectory = new Path(dataDirectory,
        partitionStrategy.getName() + "=" + name);

    if (allowCreate && !fileSystem.exists(partitionDirectory)) {
      fileSystem.mkdirs(partitionDirectory);
    }

    Builder builder = new HDFSDataset.Builder().name(name)
        .fileSystem(fileSystem).directory(directory)
        .dataDirectory(partitionDirectory).schema(schema);

    if (partitionStrategy.isPartitioned()) {
      builder.partitionStrategy(getPartitionStrategy().getPartition());
    }

    return builder.get();
  }

  @Override
  public Iterable<Dataset> getPartitions() throws IOException {
    throw new UnsupportedOperationException(
        "Attempt to get partitions for dataset:" + name);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", name).add("schema", schema)
        .add("directory", directory).add("dataDirectory", dataDirectory)
        .add("partitionStrategy", partitionStrategy).toString();
  }

  public static class Builder implements Supplier<HDFSDataset> {

    private HDFSDataset dataset;

    public Builder() {
      dataset = new HDFSDataset();
    }

    public Builder fileSystem(FileSystem fileSystem) {
      dataset.fileSystem = fileSystem;
      return this;
    }

    public Builder name(String name) {
      dataset.name = name;
      return this;
    }

    public Builder directory(Path directory) {
      dataset.directory = directory;
      return this;
    }

    public Builder dataDirectory(Path dataDirectory) {
      dataset.dataDirectory = dataDirectory;
      return this;
    }

    public Builder schema(Schema schema) {
      dataset.schema = schema;
      return this;
    }

    public Builder partitionStrategy(PartitionStrategy partitionStrategy) {
      dataset.partitionStrategy = partitionStrategy;
      return this;
    }

    @Override
    public HDFSDataset get() {
      Preconditions.checkState(dataset.name != null, "No dataset name defined");
      Preconditions.checkState(dataset.schema != null,
          "No dataset schema defined");
      Preconditions.checkState(dataset.directory != null,
          "No dataset directory defined");
      Preconditions.checkState(dataset.dataDirectory != null,
          "No dataset data directory defined");
      Preconditions.checkState(dataset.fileSystem != null,
          "No filesystem defined");

      HDFSDataset current = dataset;
      dataset = new HDFSDataset();

      return current;
    }
  }

}
