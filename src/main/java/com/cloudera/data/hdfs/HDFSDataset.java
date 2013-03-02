package com.cloudera.data.hdfs;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.data.Dataset;
import com.cloudera.data.DatasetReader;
import com.cloudera.data.DatasetWriter;
import com.cloudera.data.PartitionKey;
import com.cloudera.data.PartitionStrategy;
import com.cloudera.data.PartitionedDatasetWriter;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

public class HDFSDataset implements Dataset {

  private static final Logger logger = LoggerFactory
      .getLogger(HDFSDataset.class);

  private FileSystem fileSystem;
  private Path directory;
  private Path dataDirectory;
  private String name;
  private Schema schema;
  private PartitionStrategy partitionStrategy;
  private boolean isRoot = true;

  @Override
  public <E> DatasetWriter<E> getWriter() {
    DatasetWriter<E> writer = null;

    if (isPartitioned() && isRoot) {
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
  public <E> DatasetReader<E> getReader() throws IOException {
    List<Path> paths = Lists.newArrayList();
    accumulateDatafilePaths(dataDirectory, paths);
    return new MultiFileDatasetReader<E>(fileSystem, paths, schema);
  }

  private void accumulateDatafilePaths(Path directory, List<Path> paths)
      throws IOException {
    for (FileStatus status : fileSystem.listStatus(directory)) {
      if (status.isDirectory()) {
        accumulateDatafilePaths(status.getPath(), paths);
      } else {
        paths.add(status.getPath());
      }
    }
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
  public PartitionStrategy getPartitionStrategy() {
    return partitionStrategy;
  }

  @Override
  public boolean isPartitioned() {
    return partitionStrategy != null;
  }

  @Override
  public Dataset getPartition(PartitionKey key, boolean allowCreate)
      throws IOException {

    Preconditions.checkState(isPartitioned(),
        "Attempt to get a partition on a non-partitioned dataset (name:%s)",
        name);

    logger.debug("Loading partition:{}.{} allowCreate:{}", new Object[] {
        partitionStrategy.getName(), name, allowCreate });

    Path partitionDirectory = dataDirectory;
    for (int i = 0; i < key.getValues().size(); i++) {
      String fieldName = partitionStrategy.getFieldPartitioners().get(i)
          .getName();
      partitionDirectory = new Path(partitionDirectory, fieldName + "="
          + key.getValues().get(i));
    }

    if (allowCreate && !fileSystem.exists(partitionDirectory)) {
      fileSystem.mkdirs(partitionDirectory);
    }

    Builder builder = new HDFSDataset.Builder()
        .name(name)
        .fileSystem(fileSystem)
        .directory(directory)
        .dataDirectory(partitionDirectory)
        .schema(schema)
        .isRoot(false)
        .partitionStrategy(
            partitionStrategy.getSubpartitionStrategy(key.getLength()));

    return builder.get();
  }

  @Override
  public Iterable<Dataset> getPartitions() throws IOException {
    Preconditions.checkState(isPartitioned(),
        "Attempt to get partitions on a non-partitioned dataset (name:%s)",
        name);
    List<Path> partitionDirectories = Lists.newArrayList(dataDirectory);
    for (int i = 0; i < partitionStrategy.getFieldPartitioners().size(); i++) {
      List<Path> subpaths = Lists.newArrayList();
      for (Path p : partitionDirectories) {
        for (FileStatus stat : fileSystem.listStatus(p)) {
          subpaths.add(stat.getPath());
        }
      }
      partitionDirectories = subpaths;
    }
    List<Dataset> partitions = Lists.newArrayList();
    for (Path p : partitionDirectories) {
      Builder builder = new HDFSDataset.Builder().name(name)
          .fileSystem(fileSystem).directory(directory).schema(schema)
          .isRoot(false).dataDirectory(p);
      partitions.add(builder.get());
    }
    return partitions;
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

    public Builder isRoot(boolean isRoot) {
      dataset.isRoot = isRoot;
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
