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
package org.kitesdk.data.spi.filesystem;

import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.spi.ReaderWriterState;
import org.kitesdk.data.spi.AbstractDatasetReader;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import org.kitesdk.data.spi.DataModelUtil;

class FileSystemDatasetReader<E> extends AbstractDatasetReader<E> {

  private final FileSystem fileSystem;
  private final Path path;
  private final Schema schema;
  private final Class<E> type;

  private ReaderWriterState state;
  private DataFileReader<E> reader;

  private static final Logger LOG = LoggerFactory
    .getLogger(FileSystemDatasetReader.class);

  public FileSystemDatasetReader(FileSystem fileSystem, Path path, Schema schema,
      Class<E> type) {
    Preconditions.checkArgument(fileSystem != null, "FileSystem cannot be null");
    Preconditions.checkArgument(path != null, "Path cannot be null");
    Preconditions.checkArgument(schema != null, "Schema cannot be null");

    this.fileSystem = fileSystem;
    this.path = path;
    this.schema = schema;
    this.type = type;

    this.state = ReaderWriterState.NEW;
  }

  @Override
  public void initialize() {
    Preconditions.checkState(state.equals(ReaderWriterState.NEW),
      "A reader may not be opened more than once - current state:%s", state);

    LOG.debug("Opening reader on path:{}", path);

    try {
      reader = new DataFileReader<E>(new AvroFSInput(fileSystem.open(path),
        fileSystem.getFileStatus(path).getLen()),
          DataModelUtil.getDatumReaderForType(type, schema));
    } catch (IOException e) {
      throw new DatasetIOException("Unable to create reader path:" + path, e);
    }

    state = ReaderWriterState.OPEN;
  }

  @Override
  public boolean hasNext() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
      "Attempt to read from a file in state:%s", state);
    return reader.hasNext();
  }

  @Override
  public E next() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
      "Attempt to read from a file in state:%s", state);

    E record = DataModelUtil.createRecord(type, schema);
    try {
      return reader.next(record);
    } catch (IOException ex) {
      throw new DatasetIOException("Cannot advance reader", ex);
    }
  }

  @Override
  public void close() {
    if (!state.equals(ReaderWriterState.OPEN)) {
      return;
    }

    LOG.debug("Closing reader on path:{}", path);

    try {
      reader.close();
    } catch (IOException e) {
      throw new DatasetIOException("Unable to close reader path:" + path, e);
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
      .add("path", path)
      .add("schema", schema)
      .add("state", state)
      .add("reader", reader)
      .toString();
  }

}
