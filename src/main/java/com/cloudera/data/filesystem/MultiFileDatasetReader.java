package com.cloudera.data.filesystem;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.data.DatasetReader;

class MultiFileDatasetReader<E> implements DatasetReader<E> {

  private FileSystem fileSystem;
  private Schema schema;

  private Iterator<Path> filesIter;
  private FileSystemDatasetReader<E> reader;

  private ReaderWriterState state;

  public MultiFileDatasetReader(FileSystem fileSystem, List<Path> files,
      Schema schema) {

    this.fileSystem = fileSystem;
    this.schema = schema;
    this.filesIter = files.iterator();

    this.state = ReaderWriterState.NEW;
  }

  @Override
  public void open() throws IOException {
    Preconditions.checkState(state.equals(ReaderWriterState.NEW),
        "A reader may not be opened more than once - current state:%s", state);

    if (filesIter.hasNext()) {
      reader = new FileSystemDatasetReader<E>(fileSystem, filesIter.next(),
          schema);
      reader.open();
    }
    this.state = ReaderWriterState.OPEN;
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", justification = "Checked by Preconditions")
  public boolean hasNext() throws IOException {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to read from a file in state:%s", state);
    while (true) {
      if (reader == null) {
        return false;
      } else if (reader.hasNext()) {
        return true;
      } else {
        reader.close();
        reader = null;

        if (filesIter.hasNext()) {
          reader = new FileSystemDatasetReader<E>(fileSystem, filesIter.next(),
              schema);
          reader.open();
        } else {
          return false;
        }
      }
    }
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", justification = "Checked by Preconditions")
  public E read() throws IOException {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to read from a file in state:%s", state);
    return reader.read();
  }

  @Override
  public void close() throws IOException {
    if (!state.equals(ReaderWriterState.OPEN)) {
      return;
    }
    if (reader != null) {
      reader.close();
    }
    state = ReaderWriterState.CLOSED;
  }

}