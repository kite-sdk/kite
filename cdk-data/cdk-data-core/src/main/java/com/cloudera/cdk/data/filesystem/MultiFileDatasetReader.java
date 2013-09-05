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
package com.cloudera.cdk.data.filesystem;

import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.DatasetReader;
import com.cloudera.cdk.data.Format;
import com.cloudera.cdk.data.Formats;
import com.cloudera.cdk.data.UnknownFormatException;
import com.cloudera.cdk.data.spi.AbstractDatasetReader;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

class MultiFileDatasetReader<E> extends AbstractDatasetReader<E> {

  private final FileSystem fileSystem;
  private final DatasetDescriptor descriptor;

  private final Iterator<Path> filesIter;
  private DatasetReader<E> reader;

  private ReaderWriterState state;

  public MultiFileDatasetReader(FileSystem fileSystem, List<Path> files,
      DatasetDescriptor descriptor) {
    Preconditions.checkArgument(fileSystem != null, "FileSystem cannot be null");
    Preconditions.checkArgument(descriptor != null, "Descriptor cannot be null");
    Preconditions.checkArgument(files != null, "Descriptor cannot be null");

    this.fileSystem = fileSystem;
    this.descriptor = descriptor;

    // verify there are no null files
    try {
      this.filesIter = ImmutableList.copyOf(files).iterator();
    } catch (NullPointerException ex) {
      throw new IllegalArgumentException("File list cannot contain null Paths");
    }

    this.state = ReaderWriterState.NEW;
  }

  @Override
  public void open() {
    Preconditions.checkState(state.equals(ReaderWriterState.NEW),
      "A reader may not be opened more than once - current state:%s", state);

    final Format format = descriptor.getFormat();
    if (!(Formats.AVRO.equals(format) || Formats.PARQUET.equals(format))) {
      throw new UnknownFormatException("Cannot open format:" + format.getName());
    }

    this.state = ReaderWriterState.OPEN;
  }

  private void openNextReader() {
    if (Formats.PARQUET.equals(descriptor.getFormat())) {
      reader = new ParquetFileSystemDatasetReader(fileSystem, filesIter.next(),
          descriptor.getSchema());
    } else {
      reader = new FileSystemDatasetReader<E>(fileSystem, filesIter.next(),
          descriptor.getSchema());
    }
    reader.open();
  }

  @Override
  public boolean hasNext() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
      "Attempt to read from a file in state:%s", state);

    while (true) {
      if (reader == null) {
        if (filesIter.hasNext()) {
          openNextReader();
        } else {
          return false;
        }
      } else {
        if (reader.hasNext()) {
          return true;
        } else {
          reader.close();
          reader = null;
        }
      }
    }
  }

  @Override
  public E next() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
      "Attempt to read from a file in state:%s", state);
    if (hasNext()) {
      // if hasNext => true, then reader is guaranteed to be non-null
      return reader.next();
    } else {
      throw new NoSuchElementException();
    }
  }

  @Override
  public void remove() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to remove from a file in state:%s", state);
    if (reader == null) {
      // If next succeeds, then reader cannot be null. If the reader is null,
      // then next did not succeed or hasNext has been used and the reader is
      // gone. We could keep track of the last reader that next returned a value
      // from, but it probably isn't worth it.
      throw new IllegalStateException(
          "Remove can only be called after next() returns a value and before calling hasNext()");
    }
    reader.remove();
  }

  @Override
  public void close() {
    if (!state.equals(ReaderWriterState.OPEN)) {
      return;
    }
    if (reader != null) {
      reader.close();
    }
    state = ReaderWriterState.CLOSED;
  }

  @Override
  public boolean isOpen() {
    return state.equals(ReaderWriterState.OPEN);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("fileSystem", fileSystem)
      .add("descriptor", descriptor)
      .add("filesIter", filesIter)
      .add("reader", reader)
      .add("state", state)
      .toString();
  }

}
