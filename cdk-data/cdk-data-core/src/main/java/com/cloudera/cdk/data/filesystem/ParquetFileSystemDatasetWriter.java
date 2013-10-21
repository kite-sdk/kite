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

import com.cloudera.cdk.data.spi.ReaderWriterState;
import com.cloudera.cdk.data.DatasetWriter;
import com.cloudera.cdk.data.DatasetWriterException;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;

class ParquetFileSystemDatasetWriter<E extends IndexedRecord> implements DatasetWriter<E> {

  private static final Logger logger = LoggerFactory
    .getLogger(ParquetFileSystemDatasetWriter.class);
  private static final int DEFAULT_BLOCK_SIZE = 50 * 1024 * 1024;

  private Path path;
  private Schema schema;
  private FileSystem fileSystem;
  private boolean enableCompression;

  private Path pathTmp;
  private AvroParquetWriter<E> avroParquetWriter;
  private ReaderWriterState state;

  public ParquetFileSystemDatasetWriter(FileSystem fileSystem, Path path,
      Schema schema) {
    this(fileSystem, path, schema, true);
  }

  public ParquetFileSystemDatasetWriter(FileSystem fileSystem, Path path,
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

    try {
      CompressionCodecName codecName = CompressionCodecName.UNCOMPRESSED;
      if (enableCompression) {
         if (SnappyCodec.isNativeCodeLoaded()) {
           codecName = CompressionCodecName.SNAPPY;
         } else {
           logger.warn("Compression enabled, but Snappy native code not loaded. " +
               "Parquet file will not be compressed.");
         }
      }
      avroParquetWriter = new AvroParquetWriter<E>(fileSystem.makeQualified(pathTmp),
          schema, codecName, DEFAULT_BLOCK_SIZE,
          ParquetWriter.DEFAULT_PAGE_SIZE);
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
      avroParquetWriter.write(entity);
    } catch (IOException e) {
      throw new DatasetWriterException(
        "Unable to write entity:" + entity + " with writer:" + avroParquetWriter, e);
    }
  }

  @Override
  public void flush() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
      "Attempt to write to a writer in state:%s", state);

    // Parquet doesn't (currently) expose a flush operation
  }

  @Override
  public void close() {
    if (state.equals(ReaderWriterState.OPEN)) {
      logger.debug("Closing pathTmp:{}", pathTmp);

      try {
        Closeables.close(avroParquetWriter, false);
      } catch (IOException e) {
        throw new DatasetWriterException(
          "Unable to close writer:" + avroParquetWriter + " to path:" + pathTmp);
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
      .add("pathTmp", pathTmp)
      .add("avroParquetWriter", avroParquetWriter)
      .add("state", state)
      .toString();
  }

}
