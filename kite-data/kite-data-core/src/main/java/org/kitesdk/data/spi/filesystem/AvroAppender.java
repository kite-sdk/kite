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
import org.kitesdk.compat.Hadoop;

class AvroAppender<E> implements FileSystemWriter.FileAppender<E> {

  private final Schema schema;
  private final FileSystem fileSystem;
  private final Path path;
  private final boolean enableCompression;

  private FSDataOutputStream out = null;
  private DataFileWriter<E> dataFileWriter = null;
  private DatumWriter<E> writer = null;

  public AvroAppender(FileSystem fileSystem, Path path, Schema schema, boolean enableCompression) {
    this.fileSystem = fileSystem;
    this.path = path;
    this.schema = schema;
    this.enableCompression = enableCompression;
  }

  @Override
  public void open() throws IOException {
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

    out = fileSystem.create(path, true);
    dataFileWriter.create(schema, out);
  }

  @Override
  public void append(E entity) throws IOException {
    dataFileWriter.append(entity);
  }

  @Override
  public void flush() throws IOException {
    // Avro sync forces the end of the current block so the data is recoverable
    dataFileWriter.flush();
    Hadoop.FSDataOutputStream.hflush.invoke(out);
  }

  @Override
  public void sync() throws IOException {
    flush();
    Hadoop.FSDataOutputStream.hsync.invoke(out);
  }

  @Override
  public void close() throws IOException {
    Closeables.close(dataFileWriter, false);
  }

  @Override
  public void cleanup() throws IOException {
    // No cleanup tasks needed
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("path", path)
      .add("schema", schema)
      .add("fileSystem", fileSystem)
      .add("enableCompression", enableCompression)
      .add("dataFileWriter", dataFileWriter)
      .add("writer", writer)
      .toString();
  }
}
