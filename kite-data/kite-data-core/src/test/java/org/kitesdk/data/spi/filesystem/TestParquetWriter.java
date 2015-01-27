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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Flushable;
import org.kitesdk.data.LocalFileSystem;
import org.kitesdk.data.Syncable;

public class TestParquetWriter extends TestFileSystemWriters {
  @Override
  public FileSystemWriter<Record> newWriter(Path directory, Schema schema) {
    return FileSystemWriter.newWriter(fs, directory,
        new DatasetDescriptor.Builder()
            .schema(schema)
            .format("parquet")
            .build());
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
        fs, new Path("/tmp"),
        new DatasetDescriptor.Builder()
            .property("parquet.block.size", "34343434")
            .schema(SchemaBuilder.record("test").fields()
                .requiredString("s")
                .endRecord())
            .format("parquet")
            .build());
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
        fs, new Path("/tmp"),
        new DatasetDescriptor.Builder()
            .property(FileSystemProperties.NON_DURABLE_PARQUET_PROP, "false")
            .schema(SchemaBuilder.record("test").fields()
                .requiredString("s")
                .endRecord())
            .format("parquet")
            .build());
    Assert.assertEquals("Disabling the non-durable parquet appender should get us a durable appender",
        DurableParquetAppender.class, writer.newAppender(testDirectory).getClass());
  }

  @Test
  public void testConfigureNonDurableParquetAppender() throws IOException {
    FileSystem fs = LocalFileSystem.getInstance();
    FileSystemWriter<Object> writer = FileSystemWriter.newWriter(
        fs, new Path("/tmp"),
        new DatasetDescriptor.Builder()
            .property(FileSystemProperties.NON_DURABLE_PARQUET_PROP, "true")
            .schema(SchemaBuilder.record("test").fields()
                .requiredString("s")
                .endRecord())
            .format("parquet")
            .build());
    Assert.assertEquals("Enabling the non-durable parquet appender should get us a non-durable appender",
        ParquetAppender.class, writer.newAppender(testDirectory).getClass());
  }
}
