package com.cloudera.data.hdfs;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.data.Dataset;
import com.cloudera.data.DatasetReader;
import com.cloudera.data.DatasetWriter;
import com.cloudera.data.Partition;
import com.cloudera.data.PartitionExpression;
import com.cloudera.data.PartitionedDatasetWriter;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;

public class HDFSDataset implements Dataset {

  private static final Logger logger = LoggerFactory
      .getLogger(HDFSDataset.class);

  private FileSystem fileSystem;
  private Path dataDirectory;
  private String name;
  private Schema schema;
  private PartitionExpression partitionExpression;
  private Map<String, String> properties;

  public HDFSDataset() {
    properties = Maps.newHashMap();
  }

  @Override
  public <E> DatasetWriter<E> getWriter() {
    DatasetWriter<E> writer = null;

    if (isPartitioned()) {
      // FIXME: Why does this complain about a resource leak and now others?
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
    return partitionExpression;
  }

  @Override
  public boolean isPartitioned() {
    return partitionExpression != null;
  }

  @Override
  public <E> Partition<E> getPartition(String name, boolean allowCreate)
      throws IOException {

    Preconditions.checkState(isPartitioned(),
        "Attempt to get a partition on a non-partitioned dataset (name:%s)",
        name);

    logger.debug("Loading partition name:{} allowCreate:{}", name, allowCreate);

    Path partitionDirectory = new Path(dataDirectory, name);

    if (allowCreate && !fileSystem.exists(partitionDirectory)) {
      fileSystem.mkdirs(partitionDirectory);
    }

    return new HDFSPartition.Builder<E>().fileSystem(fileSystem)
        .directory(new Path(dataDirectory, name)).schema(schema).get();
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", name).add("schema", schema)
        .add("dataDirectory", dataDirectory)
        .add("partitionExpression", partitionExpression)
        .add("properties", properties).toString();
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

    public Builder dataDirectory(Path dataDirectory) {
      dataset.dataDirectory = dataDirectory;
      return this;
    }

    public Builder schema(Schema schema) {
      dataset.schema = schema;
      return this;
    }

    public Builder partitionExpression(PartitionExpression partitionExpression) {
      dataset.partitionExpression = partitionExpression;
      return this;
    }

    public Builder properties(Map<String, String> properties) {
      dataset.properties = properties;
      return this;
    }

    @Override
    public HDFSDataset get() {
      Preconditions.checkState(dataset.name != null, "No dataset name defined");
      Preconditions.checkState(dataset.schema != null,
          "No dataset schema defined");
      Preconditions.checkState(dataset.fileSystem != null,
          "No filesystem defined");

      HDFSDataset current = dataset;
      dataset = new HDFSDataset();

      return current;
    }
  }

}
