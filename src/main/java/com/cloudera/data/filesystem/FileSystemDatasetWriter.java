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
import java.io.Flushable;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.data.DatasetWriter;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.io.Closeables;

class FileSystemDatasetWriter<E> implements DatasetWriter<E>, Flushable,
    Closeable {

  private static final Logger logger = LoggerFactory
      .getLogger(FileSystemDatasetWriter.class);

  private Path path;
  private Schema schema;
  private FileSystem fileSystem;
  private boolean enableCompression;

  private Path pathTmp;
  private DataFileWriter<E> dataFileWriter;
  private DatumWriter<E> writer;
  private ReaderWriterState state;

  public FileSystemDatasetWriter(FileSystem fileSystem, Path path,
      Schema schema, boolean enableCompression) {

    this.fileSystem = fileSystem;
    this.path = path;
    this.pathTmp = new Path(path.getParent(), "." + path.getName() + ".tmp");
    this.schema = schema;
    this.enableCompression = enableCompression;
    this.state = ReaderWriterState.NEW;
  }

  @Override
  public void open() throws IOException {
    Preconditions.checkState(state.equals(ReaderWriterState.NEW),
        "Unable to open a writer from state:%s", state);

    logger.debug(
        "Opening data file with pathTmp:{} (final path will be path:{})",
        pathTmp, path);

    writer = new ReflectDatumWriter<E>();
    dataFileWriter = new DataFileWriter<E>(writer);

    /*
     * We may want to expose the codec in the writer and simply rely on the
     * builder and proper instantiation from dataset-level configuration.
     * Hard-coding snappy seems a little too draconian.
     */
    if (enableCompression) {
      dataFileWriter.setCodec(CodecFactory.snappyCodec());
    }

    dataFileWriter.create(schema, fileSystem.create(pathTmp, true));

    state = ReaderWriterState.OPEN;
  }

  @Override
  public void write(E entity) throws IOException {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to write to a writer in state:%s", state);

    dataFileWriter.append(entity);
  }

  @Override
  public void flush() throws IOException {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to write to a writer in state:%s", state);

    dataFileWriter.flush();
  }

  @Override
  public void close() throws IOException {
    if (state.equals(ReaderWriterState.OPEN)) {
      logger.debug("Closing pathTmp:{}", pathTmp);

      Closeables.close(dataFileWriter, false);

      logger.debug("Committing pathTmp:{} to path:{}", pathTmp, path);

      if (!fileSystem.rename(pathTmp, path)) {
        throw new IOException("Failed to move " + pathTmp + " to " + path);
      }

      state = ReaderWriterState.CLOSED;
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("path", path)
        .add("pathTmp", pathTmp).add("schema", schema)
        .add("enableCompression", enableCompression).add("state", state)
        .toString();
  }

  public static class Builder<E> implements
      Supplier<FileSystemDatasetWriter<E>> {

    private FileSystem fileSystem;
    private Path path;
    private Schema schema;
    private boolean enableCompression;

    public Builder() {
      enableCompression = true;
    }

    public Builder<E> fileSystem(FileSystem fileSystem) {
      this.fileSystem = fileSystem;
      return this;
    }

    public Builder<E> path(Path path) {
      this.path = path;
      return this;
    }

    public Builder<E> schema(Schema schema) {
      this.schema = schema;
      return this;
    }

    public Builder<E> enableCompression(boolean enableCompression) {
      this.enableCompression = enableCompression;
      return this;
    }

    @Override
    public FileSystemDatasetWriter<E> get() {
      Preconditions
          .checkState(fileSystem != null, "File system is not defined");
      Preconditions.checkState(path != null, "Path is not defined");
      Preconditions.checkState(schema != null, "Schema is not defined");

      FileSystemDatasetWriter<E> writer = new FileSystemDatasetWriter<E>(
          fileSystem, path, schema, enableCompression);

      return writer;
    }

  }

}
