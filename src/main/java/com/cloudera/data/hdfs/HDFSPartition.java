package com.cloudera.data.hdfs;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.data.DatasetReader;
import com.cloudera.data.DatasetWriter;
import com.cloudera.data.Partition;
import com.cloudera.data.PartitionExpression;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;

public class HDFSPartition<E> implements Partition<E> {

  private String name;

  private Schema schema;
  private FileSystem fileSystem;
  private Path directory;

  @Override
  public String getName() {
    return name;
  }

  @Override
  public PartitionExpression getExpression() {
    return null;
  }

  @Override
  public DatasetReader<E> getReader() {
    throw new UnsupportedOperationException(
        "Attempt to get a reader for partition:" + name);
  }

  @Override
  public DatasetWriter<E> getWriter() throws IOException {
    // FIXME: This file name is not guaranteed to be truly unique.
    Path dataFile = new Path(directory, Joiner.on('-').join(
        System.currentTimeMillis(), Thread.currentThread().getId()));

    return new HDFSDatasetWriter.Builder<E>().fileSystem(fileSystem)
        .path(dataFile).schema(schema).get();

  }

  public static class Builder<E> implements Supplier<HDFSPartition<E>> {

    private HDFSPartition<E> partition;

    public Builder() {
      partition = new HDFSPartition<E>();
    }

    public Builder<E> fileSystem(FileSystem fileSystem) {
      partition.fileSystem = fileSystem;
      return this;
    }

    public Builder<E> directory(Path directory) {
      partition.directory = directory;
      return this;
    }

    public Builder<E> schema(Schema schema) {
      partition.schema = schema;
      return this;
    }

    @Override
    public HDFSPartition<E> get() {
      Preconditions.checkState(partition.fileSystem != null, "");
      Preconditions.checkState(partition.directory != null, "");
      Preconditions.checkState(partition.schema != null, "");

      HDFSPartition<E> current = partition;
      partition = new HDFSPartition<E>();

      return current;
    }

  }

}
