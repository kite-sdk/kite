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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.data.DatasetReader;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

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

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("reader", reader)
        .add("state", state).add("filesIter", filesIter).add("schema", schema)
        .toString();
  }

}