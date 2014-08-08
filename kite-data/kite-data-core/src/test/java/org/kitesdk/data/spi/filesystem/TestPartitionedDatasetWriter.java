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

import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.spi.MetadataProvider;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import java.io.IOException;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.kitesdk.data.spi.filesystem.DatasetTestUtilities.USER_SCHEMA;

public class TestPartitionedDatasetWriter {

  private Configuration conf;
  private FileSystem fileSystem;
  private Path testDirectory;
  private FileSystemDatasetRepository repo;
  private PartitionedDatasetWriter<Object> writer;

  @Before
  public void setUp() throws IOException {
    this.conf = new Configuration();
    this.fileSystem = FileSystem.get(conf);
    this.testDirectory = new Path(Files.createTempDir().getAbsolutePath());
    this.repo = new FileSystemDatasetRepository(conf, testDirectory);

    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder()
        .hash("username", 2).build();
    FileSystemDataset<Object> users = (FileSystemDataset<Object>) repo.create(
        "ns", "users",
        new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .partitionStrategy(partitionStrategy)
            .build());
    writer = new PartitionedDatasetWriter<Object>(
        new FileSystemView<Object>(users, Object.class));
  }

  @After
  public void tearDown() throws IOException {
    fileSystem.delete(testDirectory, true);
  }

  @Test
  public void testBasicInitClose() throws IOException {
    writer.initialize();
    writer.close();
  }

  @Test
  public void testWriter() throws IOException {
    Record record = new GenericRecordBuilder(USER_SCHEMA)
        .set("username", "test1").set("email", "a@example.com").build();
    try {
      writer.initialize();
      writer.write(record);
      writer.flush();
      writer.close();
    } finally {
      Closeables.close(writer, true);
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testWriteToClosedWriterFails() throws IOException {
    Record record = new GenericRecordBuilder(USER_SCHEMA)
        .set("username", "test1").set("email", "a@example.com").build();
    writer.initialize();
    writer.close();
    writer.write(record);
  }

}
