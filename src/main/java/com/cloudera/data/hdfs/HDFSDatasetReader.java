package com.cloudera.data.hdfs;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.data.DatasetReader;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class HDFSDatasetReader<E> implements DatasetReader<E>, Closeable {

  private FileSystem fileSystem;
  private Path path;

  private State state;

  private static final Logger logger = LoggerFactory
      .getLogger(HDFSDatasetReader.class);

  public HDFSDatasetReader() {
    state = State.NEW;
  }

  @Override
  public void open() {
    Preconditions.checkState(state.equals(State.NEW),
        "A reader may not be opened more than once - current state:%s", state);

    state = State.OPEN;
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public E read() {
    Preconditions.checkState(state.equals(State.OPEN),
        "Attempt to read from a file in state:%s", state);

    return null;
  }

  @Override
  public void close() throws IOException {
    if (!state.equals(State.OPEN)) {
      return;
    }

    logger.debug("Closing reader on path:{}", path);
    state = State.CLOSED;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("path", path).add("state", state)
        .toString();
  }

  private static enum State {
    NEW, OPEN, CLOSED
  }

}
