package com.cloudera.data.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.data.DatasetReader;
import com.cloudera.data.DatasetWriter;
import com.cloudera.data.Partition;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

public class HDFSPartition<E> implements Partition<E> {

  private static final Logger logger = LoggerFactory
      .getLogger(HDFSPartition.class);

  private Schema schema;
  private FileSystem fileSystem;
  private Path directory;

  private List<Path> files;

  @Override
  public DatasetReader<E> getReader() throws FileNotFoundException, IOException {
    if (files == null) {
      logger.debug("First request for a reader - building file list");

      files = Lists.newLinkedList();

      RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(
          directory, true);

      while (iterator.hasNext()) {
        LocatedFileStatus fileStatus = iterator.next();

        files.add(fileStatus.getPath());
        logger.debug("adding {} to the partition file list",
            fileStatus.getPath());
      }
    }

    return new MultiFileDatasetReader<E>(fileSystem, files, schema);
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
