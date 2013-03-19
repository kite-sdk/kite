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
package com.cloudera.data.filesystem;

import com.cloudera.data.Dataset;
import com.cloudera.data.DatasetDescriptor;
import com.cloudera.data.DatasetReader;
import com.cloudera.data.DatasetWriter;
import com.cloudera.data.PartitionKey;
import com.cloudera.data.PartitionStrategy;
import com.cloudera.data.impl.Accessor;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FileSystemDataset implements Dataset {

  private static final Logger logger = LoggerFactory
      .getLogger(FileSystemDataset.class);

  private final FileSystem fileSystem;
  private final Path directory;
  private final Path dataDirectory;
  private final String name;
  private final DatasetDescriptor descriptor;
  private final PartitionKey partitionKey;

  private final PartitionStrategy partitionStrategy;
  private final Schema schema;

  FileSystemDataset(FileSystem fileSystem, Path directory, Path dataDirectory,
      String name, DatasetDescriptor descriptor, PartitionKey partitionKey,
      PartitionStrategy partitionStrategy, Schema schema) {
    this.fileSystem = fileSystem;
    this.directory = directory;
    this.dataDirectory = dataDirectory;
    this.name = name;
    this.descriptor = descriptor;
    this.partitionKey = partitionKey;
    this.partitionStrategy = partitionStrategy;
    this.schema = schema;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public DatasetDescriptor getDescriptor() {
    return descriptor;
  }

  @Override
  public <E> DatasetWriter<E> getWriter() {
    logger.debug("Getting writer to dataset:{}", this);

    DatasetWriter<E> writer = null;

    if (descriptor.isPartitioned()) {
      writer = new PartitionedDatasetWriter<E>(this);
    } else {
      Path dataFile = new Path(dataDirectory, uniqueFilename());
      writer = new FileSystemDatasetWriter.Builder<E>().fileSystem(fileSystem)
          .path(dataFile).schema(schema).get();
    }

    return writer;
  }

  @Override
  public <E> DatasetReader<E> getReader() throws IOException {
    logger.debug("Getting reader for dataset:{}", this);
    List<Path> paths = Lists.newArrayList();
    accumulateDatafilePaths(dataDirectory, paths);
    return new MultiFileDatasetReader<E>(fileSystem, paths, schema);
  }

  @Override
  @Nullable
  public Dataset getPartition(PartitionKey key, boolean allowCreate)
      throws IOException {
    Preconditions.checkState(descriptor.isPartitioned(),
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

    int partitionDepth = key.getLength();
    PartitionStrategy subpartitionStrategy = Accessor.getDefault()
        .getSubpartitionStrategy(partitionStrategy, partitionDepth);

    return new FileSystemDataset.Builder()
        .name(name)
        .fileSystem(fileSystem)
        .descriptor(
            new DatasetDescriptor.Builder().schema(schema)
                .partitionStrategy(subpartitionStrategy).get())
        .directory(directory).dataDirectory(partitionDirectory)
        .partitionKey(key).get();
  }

  @Override
  public Iterable<Dataset> getPartitions() throws IOException {
    Preconditions.checkState(descriptor.isPartitioned(),
        "Attempt to get partitions on a non-partitioned dataset (name:%s)",
        name);

    List<Dataset> partitions = Lists.newArrayList();

    for (FileStatus stat : fileSystem.listStatus(dataDirectory)) {
      Path p = stat.getPath();
      PartitionKey key = fromDirectoryName(p);
      Builder builder = new FileSystemDataset.Builder()
          .name(name)
          .fileSystem(fileSystem)
          .descriptor(
              new DatasetDescriptor.Builder()
                  .schema(schema)
                  .partitionStrategy(
                      Accessor.getDefault().getSubpartitionStrategy(
                          partitionStrategy, 1)).get()).directory(directory)
          .dataDirectory(p).partitionKey(key);

      partitions.add(builder.get());
    }

    return partitions;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", name)
        .add("descriptor", descriptor).add("directory", directory)
        .add("dataDirectory", dataDirectory).add("partitionKey", partitionKey)
        .toString();
  }

  private String uniqueFilename() {
    // FIXME: This file name is not guaranteed to be truly unique.
    return Joiner.on('-').join(System.currentTimeMillis(),
        Thread.currentThread().getId() + ".avro");
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

    if (partitionKey != null) {
      values.addAll(partitionKey.getValues());
    }

    values.add(Splitter.on('=').split(dir.getName()));

    return Accessor.getDefault().newPartitionKey(values.toArray());
  }

  public static class Builder implements Supplier<FileSystemDataset> {

    private FileSystem fileSystem;
    private Path directory;
    private Path dataDirectory;
    private String name;
    private DatasetDescriptor descriptor;
    private PartitionKey partitionKey;

    private PartitionStrategy partitionStrategy;
    private Schema schema;

    public Builder fileSystem(FileSystem fileSystem) {
      this.fileSystem = fileSystem;
      return this;
    }

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder directory(Path directory) {
      this.directory = directory;
      return this;
    }

    public Builder dataDirectory(Path dataDirectory) {
      this.dataDirectory = dataDirectory;
      return this;
    }

    public Builder descriptor(DatasetDescriptor descriptor) {
      this.descriptor = descriptor;
      return this;
    }

    Builder partitionKey(@Nullable PartitionKey partitionKey) {
      this.partitionKey = partitionKey;
      return this;
    }

    @Override
    public FileSystemDataset get() {
      Preconditions.checkState(this.name != null, "No dataset name defined");
      Preconditions.checkState(this.descriptor != null,
          "No dataset descriptor defined");
      Preconditions.checkState(this.directory != null,
          "No dataset directory defined");
      Preconditions.checkState(this.dataDirectory != null,
          "No dataset data directory defined");
      Preconditions
          .checkState(this.fileSystem != null, "No filesystem defined");

      this.schema = this.descriptor.getSchema();

      if (this.descriptor.isPartitioned()) {
        this.partitionStrategy = this.descriptor.getPartitionStrategy();
      }

      return new FileSystemDataset(fileSystem, directory, dataDirectory, name,
          descriptor, partitionKey, partitionStrategy, schema);
    }
  }

}
