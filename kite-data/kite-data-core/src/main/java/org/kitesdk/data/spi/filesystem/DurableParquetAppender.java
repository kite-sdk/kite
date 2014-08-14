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

import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.io.Closeables;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;

class DurableParquetAppender<E extends IndexedRecord> implements FileSystemWriter.FileAppender<E> {

  private static final Logger LOG = LoggerFactory
      .getLogger(DurableParquetAppender.class);

  private final Path path;
  private final Path avroPath;
  private final AvroAppender<E> avroAppender;
  private final ParquetAppender<E> parquetAppender;
  private final Schema schema;
  private final FileSystem fs;

  public DurableParquetAppender(FileSystem fs, Path path, Schema schema,
                                Configuration conf, boolean enableCompression) {
    this.fs = fs;
    this.path = path;
    this.schema = schema;
    this.avroPath = avroPath(path);
    this.avroAppender = new AvroAppender<E>(
        fs, avroPath, schema, enableCompression);
    this.parquetAppender = new ParquetAppender<E>(
        fs, path, schema, conf, enableCompression);
  }

  @Override
  public void open() throws IOException {
    avroAppender.open();
    parquetAppender.open();
  }

  @Override
  public void append(E entity) throws IOException {
    avroAppender.append(entity);
    parquetAppender.append(entity);
  }

  @Override
  public void flush() throws IOException {
    avroAppender.flush();
  }

  @Override
  public void close() throws IOException {
    Closeables.close(avroAppender, false);
    Closeables.close(parquetAppender, false);
  }

  @Override
  public void cleanup() throws IOException {
    // this is called after the parquet file is committed
    // the avro copy is no longer needed
    fs.delete(avroPath, false /* no recursion, should be a file */ );
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("fs", fs)
        .add("path", path)
        .add("schema", schema)
        .add("avroPath", avroPath)
        .toString();
  }

  private static Path avroPath(Path path) {
    return new Path(path.getParent(), path.getName() + ".avro");
  }
}
