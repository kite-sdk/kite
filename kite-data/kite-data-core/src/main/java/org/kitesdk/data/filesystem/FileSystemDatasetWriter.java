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
package org.kitesdk.data.filesystem;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.DatasetWriterException;
import org.kitesdk.data.spi.DynMethods;
import org.kitesdk.data.spi.ReaderWriterState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FileSystemDatasetWriter<E> implements DatasetWriter<E> {

  private static final Logger logger = LoggerFactory
    .getLogger(FileSystemDatasetWriter.class);

  private static DynMethods.UnboundMethod hflush = new DynMethods.Builder("hflush")
      .impl(FSDataOutputStream.class, "sync")
      .impl(FSDataOutputStream.class, "hflush")
      .build();

  private static DynMethods.UnboundMethod hsync = new DynMethods.Builder("hsync")
      .impl(FSDataOutputStream.class, "hsync")
      .defaultNoop() // no hadoop-1 equivalent
      .build();

  private Path path;
  private Schema schema;
  private FileSystem fileSystem;
  private boolean enableCompression;

  private Path pathTmp;
  private FSDataOutputStream out;
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
  public void open() {
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

    try {
      out = fileSystem.create(pathTmp, true);
      dataFileWriter.create(schema, out);
    } catch (IOException e) {
      throw new DatasetWriterException("Unable to create writer to path:" + pathTmp, e);
    }

    state = ReaderWriterState.OPEN;
  }

  @Override
  public void write(E entity) {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
      "Attempt to write to a writer in state:%s", state);

    try {
      dataFileWriter.append(entity);
    } catch (IOException e) {
      throw new DatasetWriterException(
        "Unable to write entity:" + entity + " with writer:" + dataFileWriter, e);
    }
  }

  @Override
  public void flush() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
      "Attempt to write to a writer in state:%s", state);

    try {
      dataFileWriter.flush();
      hflush.invoke(out);
    } catch (IOException e) {
      throw new DatasetWriterException(
        "Unable to flush file writer:" + dataFileWriter);
    }
  }

  @Override
  public void close() {
    if (state.equals(ReaderWriterState.OPEN)) {
      logger.debug("Closing pathTmp:{}", pathTmp);

      try {
        Closeables.close(dataFileWriter, false);
      } catch (IOException e) {
        throw new DatasetWriterException(
          "Unable to close writer:" + dataFileWriter + " to path:" + pathTmp);
      }

      logger.debug("Committing pathTmp:{} to path:{}", pathTmp, path);

      try {
        if (!fileSystem.rename(pathTmp, path)) {
          throw new DatasetWriterException(
            "Failed to move " + pathTmp + " to " + path);
        }
      } catch (IOException e) {
        throw new DatasetWriterException(
          "Internal error while trying to commit path:" + pathTmp, e);
      }

      state = ReaderWriterState.CLOSED;
    }
  }

  @Override
  public boolean isOpen() {
    return state.equals(ReaderWriterState.OPEN);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("path", path)
      .add("schema", schema)
      .add("fileSystem", fileSystem)
      .add("enableCompression", enableCompression)
      .add("pathTmp", pathTmp)
      .add("dataFileWriter", dataFileWriter)
      .add("writer", writer)
      .add("state", state)
      .toString();
  }

  public static class Builder {

    private FileSystem fileSystem;
    private Path path;
    private Schema schema;
    private boolean enableCompression;

    public Builder() {
      enableCompression = true;
    }

    public Builder fileSystem(FileSystem fileSystem) {
      this.fileSystem = fileSystem;
      return this;
    }

    public Builder path(Path path) {
      this.path = path;
      return this;
    }

    public Builder schema(Schema schema) {
      this.schema = schema;
      return this;
    }

    public Builder enableCompression(boolean enableCompression) {
      this.enableCompression = enableCompression;
      return this;
    }

    public <E> FileSystemDatasetWriter<E> build() {
      Preconditions
        .checkState(fileSystem != null, "File system is not defined");
      Preconditions.checkState(path != null, "Path is not defined");
      Preconditions.checkState(schema != null, "Schema is not defined");

      return new FileSystemDatasetWriter<E>(
        fileSystem, path, schema, enableCompression);
    }

  }
}
