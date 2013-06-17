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

import com.cloudera.data.DatasetDescriptor;
import com.cloudera.data.DatasetReader;
import com.cloudera.data.Formats;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.Iterator;
import java.util.List;

class MultiFileDatasetReader<E> implements DatasetReader<E> {

  private final FileSystem fileSystem;
  private final DatasetDescriptor descriptor;

  private final Iterator<Path> filesIter;
  private DatasetReader<E> reader;

  private ReaderWriterState state;

  public MultiFileDatasetReader(FileSystem fileSystem, List<Path> files,
      DatasetDescriptor descriptor) {

    this.fileSystem = fileSystem;
    this.descriptor = descriptor;
    this.filesIter = files.iterator();

    this.state = ReaderWriterState.NEW;
  }

  @Override
  public void open() {
    Preconditions.checkState(state.equals(ReaderWriterState.NEW),
      "A reader may not be opened more than once - current state:%s", state);

    if (filesIter.hasNext()) {
      openNextReader();
    }
    this.state = ReaderWriterState.OPEN;
  }

  private void openNextReader() {
    if (Formats.PARQUET.equals(descriptor.getFormat())) {
      reader = new ParquetFileSystemDatasetReader<E>(fileSystem, filesIter.next(),
          descriptor.getSchema());
    } else {
      reader = new FileSystemDatasetReader<E>(fileSystem, filesIter.next(),
          descriptor.getSchema());
    }
    reader.open();
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", justification = "Checked by Preconditions")
  public boolean hasNext() {
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
          openNextReader();
        } else {
          return false;
        }
      }
    }
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", justification = "Checked by Preconditions")
  public E read() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
      "Attempt to read from a file in state:%s", state);
    return reader.read();
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