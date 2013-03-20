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

import java.io.Closeable;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.data.DatasetReader;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

class FileSystemDatasetReader<E> implements DatasetReader<E>, Closeable {

  private FileSystem fileSystem;
  private Path path;
  private Schema schema;

  private ReaderWriterState state;
  private DataFileReader<E> reader;

  private static final Logger logger = LoggerFactory
      .getLogger(FileSystemDatasetReader.class);

  public FileSystemDatasetReader(FileSystem fileSystem, Path path, Schema schema) {
    this.fileSystem = fileSystem;
    this.path = path;
    this.schema = schema;

    this.state = ReaderWriterState.NEW;
  }

  @Override
  public void open() throws IOException {
    Preconditions.checkState(state.equals(ReaderWriterState.NEW),
        "A reader may not be opened more than once - current state:%s", state);

    logger.debug("Opening reader on path:{}", path);

    reader = new DataFileReader<E>(new AvroFSInput(fileSystem.open(path),
        fileSystem.getFileStatus(path).getLen()), new ReflectDatumReader<E>(
        schema));

    state = ReaderWriterState.OPEN;
  }

  @Override
  public boolean hasNext() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to read from a file in state:%s", state);
    return reader.hasNext();
  }

  @Override
  public E read() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to read from a file in state:%s", state);

    return reader.next();
  }

  @Override
  public void close() throws IOException {
    if (!state.equals(ReaderWriterState.OPEN)) {
      return;
    }

    logger.debug("Closing reader on path:{}", path);

    reader.close();
    state = ReaderWriterState.CLOSED;
  }

  @Override
  public boolean isOpen() {
    return state.equals(ReaderWriterState.OPEN);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("path", path).add("state", state)
        .add("reader", reader).add("schema", schema).toString();
  }

}
