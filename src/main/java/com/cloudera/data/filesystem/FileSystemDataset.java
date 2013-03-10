package com.cloudera.data.filesystem;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.data.Dataset;
import com.cloudera.data.DatasetReader;
import com.cloudera.data.DatasetWriter;
import com.cloudera.data.FieldPartitioner;
import com.cloudera.data.PartitionStrategy;
import com.cloudera.data.impl.Accessor;
import com.cloudera.data.impl.PartitionKey;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

class FileSystemDataset implements Dataset {

  private static final Logger logger = LoggerFactory
      .getLogger(FileSystemDataset.class);

  private FileSystem fileSystem;
  private Path directory;
  private Path dataDirectory;
  private String name;
  private Schema schema;
  private PartitionStrategy partitionStrategy; // at root
  private PartitionKey partitionKey;

  @Override
  public <E> DatasetWriter<E> getWriter() {
    DatasetWriter<E> writer = null;

    if (partitionStrategy != null) {
      if (partitionKey == null
          || partitionKey.getLength() < partitionStrategy
              .getFieldPartitioners().size()) {
        // FIXME: Why does this complain about a resource leak and not others?
        writer = new PartitionedDatasetWriter<E>(this, partitionStrategy);
      } else {
        Path dataFile = new Path(toDirectoryName(dataDirectory, partitionKey),
            uniqueFilename());
        writer = new FileSystemDatasetWriter.Builder<E>()
            .fileSystem(fileSystem).path(dataFile).schema(schema).get();
      }
    } else {
      Path dataFile = new Path(dataDirectory, uniqueFilename());
      writer = new FileSystemDatasetWriter.Builder<E>().fileSystem(fileSystem)
          .path(dataFile).schema(schema).get();
    }

    return writer;
  }

  private String uniqueFilename() {
    // FIXME: This file name is not guaranteed to be truly unique.
    return Joiner.on('-').join(System.currentTimeMillis(),
        Thread.currentThread().getId() + ".avro");
  }

  @Override
  public <E> DatasetReader<E> getReader() throws IOException {
    List<Path> paths = Lists.newArrayList();
    accumulateDatafilePaths(dataDirectory, paths);
    pruneDatafilePaths(paths);
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

  private void pruneDatafilePaths(List<Path> paths) {
    if (partitionKey == null) {
      return;
    }
    for (Iterator<Path> it = paths.iterator(); it.hasNext();) {
      Path path = it.next();
      Path dir = path.getParent(); // directory containing leaf data file
      // walk up the directory hierarchy
      for (int i = partitionStrategy.getFieldPartitioners().size() - 1; i >= 0; i--) {
        FieldPartitioner fieldPartitioner = partitionStrategy
            .getFieldPartitioners().get(i);
        if (partitionKey.get(i) != null) {
          String filterName = fieldPartitioner.getName() + "="
              + partitionKey.get(i);
          if (!filterName.equals(dir.getName())) {
            it.remove();
            break;
          }
        }
        dir = dir.getParent();
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
    if (partitionKey == null) {
      return partitionStrategy;
    }
    return Accessor.getDefault().getSubpartitionStrategy(partitionStrategy,
        partitionKey.getLength());
  }

  @Override
  public boolean isPartitioned() {
    return getPartitionStrategy() != null;
  }

  @Override
  public <E> Dataset getPartition(E entity, boolean allowCreate)
      throws IOException {

    Preconditions.checkState(isPartitioned(),
        "Attempt to get a partition on a non-partitioned dataset (name:%s)",
        name);

    logger.debug("Loading partition for entity {}, allowCreate:{}",
        new Object[] { entity, allowCreate });

    PartitionKey key = Accessor.getDefault().getPartitionKey(partitionStrategy,
        entity);
    return getPartitionForKey(key, allowCreate);
  }

  @Nullable
  Dataset getPartitionForKey(PartitionKey key, boolean allowCreate)
      throws IOException {
    Preconditions.checkState(isPartitioned(),
        "Attempt to get a partition on a non-partitioned dataset (name:%s)",
        name);

    logger.debug("Loading partition for key {}, allowCreate:{}", new Object[] {
        key, allowCreate });

    Path partitionDirectory = toDirectoryName(dataDirectory, key);
    if (!fileSystem.exists(partitionDirectory)) {
      if (allowCreate) {
        fileSystem.mkdirs(partitionDirectory);
      } else {
        return null;
      }
    }

    return new FileSystemDataset.Builder().name(name).fileSystem(fileSystem)
        .directory(directory).dataDirectory(dataDirectory).schema(schema)
        .partitionStrategy(partitionStrategy).partitionKey(key).get();
  }

  private Path toDirectoryName(Path dir, PartitionKey key) {
    Path result = dir;
    for (int i = 0; i < key.getLength(); i++) {
      String fieldName = partitionStrategy.getFieldPartitioners().get(i)
          .getName();
      result = new Path(result, fieldName + "=" + key.get(i));
    }
    return result;
  }

  private PartitionKey fromDirectoryName(Path dir) {
    List<Object> values = Lists.newArrayList();
    // walk up the directory hierarchy
    for (int i = partitionStrategy.getFieldPartitioners().size() - 1; i >= 0; i--) {
      String value = Iterables.get(Splitter.on('=').split(dir.getName()), 1);
      values.add(0, value);
      dir = dir.getParent();
    }
    return new PartitionKey(values.toArray());
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
          stat.getPath().getName();
        }
      }
      partitionDirectories = subpaths;
    }
    List<Dataset> partitions = Lists.newArrayList();
    for (Path p : partitionDirectories) {
      PartitionKey key = fromDirectoryName(p);
      Builder builder = new FileSystemDataset.Builder().name(name)
          .fileSystem(fileSystem).directory(directory).schema(schema)
          .dataDirectory(dataDirectory).partitionStrategy(partitionStrategy)
          .partitionKey(key);
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

  public static class Builder implements Supplier<FileSystemDataset> {

    private FileSystemDataset dataset;

    public Builder() {
      dataset = new FileSystemDataset();
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

    public Builder partitionStrategy(
        @Nullable PartitionStrategy partitionStrategy) {
      dataset.partitionStrategy = partitionStrategy;
      return this;
    }

    Builder partitionKey(@Nullable PartitionKey partitionKey) {
      dataset.partitionKey = partitionKey;
      return this;
    }

    @Override
    public FileSystemDataset get() {
      Preconditions.checkState(dataset.name != null, "No dataset name defined");
      Preconditions.checkState(dataset.schema != null,
          "No dataset schema defined");
      Preconditions.checkState(dataset.directory != null,
          "No dataset directory defined");
      Preconditions.checkState(dataset.dataDirectory != null,
          "No dataset data directory defined");
      Preconditions.checkState(dataset.fileSystem != null,
          "No filesystem defined");

      FileSystemDataset current = dataset;
      dataset = new FileSystemDataset();

      return current;
    }
  }

}
