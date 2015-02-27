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

import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configuration;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.spi.DataModelUtil;
import org.kitesdk.data.spi.ReaderWriterState;
import org.kitesdk.data.spi.AbstractDatasetReader;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import java.io.EOFException;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.avro.AvroParquetReader;
import parquet.avro.AvroReadSupport;

class ParquetFileSystemDatasetReader<E extends IndexedRecord> extends AbstractDatasetReader<E> {

  private FileSystem fileSystem;
  private Path path;
  private Schema schema;
  private Schema readerSchema;
  private Class<E> type;

  private ReaderWriterState state;
  private AvroParquetReader<E> reader;

  private E next;

  private static final Logger LOG = LoggerFactory
    .getLogger(ParquetFileSystemDatasetReader.class);

  public ParquetFileSystemDatasetReader(FileSystem fileSystem, Path path,
      Schema schema, Class<E> type) {
    Preconditions.checkArgument(fileSystem != null, "FileSystem cannot be null");
    Preconditions.checkArgument(path != null, "Path cannot be null");
    Preconditions.checkArgument(schema != null, "Schema cannot be null");
    Preconditions.checkArgument(IndexedRecord.class.isAssignableFrom(type) ||
        (Class<?>)type == Object.class,
        "The entity type must implement IndexedRecord");

    this.fileSystem = fileSystem;
    this.path = path;
    this.schema = schema;
    this.type = type;
    this.readerSchema = DataModelUtil.getReaderSchema(type, schema);

    this.state = ReaderWriterState.NEW;
  }

  @Override
  public void initialize() {
    Preconditions.checkState(state.equals(ReaderWriterState.NEW),
      "A reader may not be opened more than once - current state:%s", state);

    LOG.debug("Opening reader on path:{}", path);

    try {
      final Configuration conf = fileSystem.getConf();
      AvroReadSupport.setAvroReadSchema(conf, readerSchema);
      reader = new AvroParquetReader<E>(
          conf, fileSystem.makeQualified(path));
    } catch (IOException e) {
      throw new DatasetIOException("Unable to create reader path:" + path, e);
    }

    advance();

    state = ReaderWriterState.OPEN;
  }

  @Override
  public boolean hasNext() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
      "Attempt to read from a file in state:%s", state);
    return next != null;
  }

  @Override
  public E next() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
      "Attempt to read from a file in state:%s", state);
    if (next == null) {
      throw new NoSuchElementException();
    }

    E current = next;
    advance();
    return current;
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
      this.state = ReaderWriterState.ERROR;
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

  private void advance() {
    try {
      this.next = reader.read();
    } catch (EOFException e) {
      this.next = null;
    } catch (IOException e) {
      this.state = ReaderWriterState.ERROR;
      throw new DatasetIOException("Unable to read next record from: " + path, e);
    }
  }

}
