/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spi.filesystem;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.Flushable;
import org.kitesdk.data.LocalFileSystem;
import org.kitesdk.data.Syncable;

public class TestParquetWriter extends TestFileSystemWriters {

  private static final Schema SCHEMA = SchemaBuilder.record("test").fields()
      .requiredString("s")
      .endRecord();

  @Override
  public FileSystemWriter<Record> newWriter(Path directory, Schema datasetSchema, Schema writerSchema) {
    return FileSystemWriter.newWriter(fs, directory, 100, 2 * 1024 * 1024,
        new DatasetDescriptor.Builder()
            .property(
                "kite.writer.roll-interval-seconds", String.valueOf(10))
            .property(
                "kite.writer.target-file-size",
                String.valueOf(32 * 1024 * 1024)) // 32 MB
            .schema(datasetSchema)
            .format("parquet")
            .build(), writerSchema);
  }

  @Override
  public DatasetReader<Record> newReader(Path path, Schema schema) {
    return new ParquetFileSystemDatasetReader<Record>(
        fs, path, schema, Record.class);
  }

  @Test
  public void testIsFlushable() {
    Assert.assertFalse(fsWriter instanceof Flushable);
  }

  @Test
  public void testIsSyncable() {
    Assert.assertFalse(fsWriter instanceof Syncable);
  }

  @Test
  public void testParquetConfiguration() throws IOException {
    FileSystem fs = LocalFileSystem.getInstance();
    FileSystemWriter<Object> writer = FileSystemWriter.newWriter(
        fs, new Path("/tmp"), -1, -1,
        new DatasetDescriptor.Builder()
            .property("parquet.block.size", "34343434")
            .schema(SCHEMA)
            .format("parquet")
            .build(), SCHEMA);
    Assert.assertEquals("Should copy properties to Configuration",
        34343434, writer.conf.getInt("parquet.block.size", -1));
  }

  @Test
  public void testDefaultToParquetAppender() throws IOException {
    Assert.assertEquals("Should default to non-durable parquet appender",
        ParquetAppender.class, fsWriter.newAppender(testDirectory).getClass());
  }

  @Test
  public void testConfigureDurableParquetAppender() throws IOException {
    FileSystem fs = LocalFileSystem.getInstance();
    FileSystemWriter<Object> writer = FileSystemWriter.newWriter(
        fs, new Path("/tmp"), -1, -1,
        new DatasetDescriptor.Builder()
            .property(FileSystemProperties.NON_DURABLE_PARQUET_PROP, "false")
            .schema(SCHEMA)
            .format("parquet")
            .build(), SCHEMA);
    Assert.assertEquals("Disabling the non-durable parquet appender should get us a durable appender",
        DurableParquetAppender.class, writer.newAppender(testDirectory).getClass());
  }

  @Test
  public void testConfigureNonDurableParquetAppender() throws IOException {
    FileSystem fs = LocalFileSystem.getInstance();
    FileSystemWriter<Object> writer = FileSystemWriter.newWriter(
        fs, new Path("/tmp"), -1, -1,
        new DatasetDescriptor.Builder()
            .property(FileSystemProperties.NON_DURABLE_PARQUET_PROP, "true")
            .schema(SCHEMA)
            .format("parquet")
            .build(), SCHEMA);
    Assert.assertEquals("Enabling the non-durable parquet appender should get us a non-durable appender",
        ParquetAppender.class, writer.newAppender(testDirectory).getClass());
  }

  @Override
  @Ignore // Needs PARQUET-308 to estimate current file size
  public void testTargetFileSize() throws IOException {
  }
}
