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

import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.PartitionStrategy;
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
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.View;

import static org.kitesdk.data.spi.filesystem.DatasetTestUtilities.USER_SCHEMA;

public class TestPartitionedDatasetWriter {

  private Configuration conf;
  private FileSystem fileSystem;
  private Path testDirectory;
  private FileSystemDatasetRepository repo;
  private PartitionedDatasetWriter<Object, ?> writer;

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

    writer = PartitionedDatasetWriter.newWriter(
        new FileSystemView<Object>(users, null, null, Object.class));
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

  @Test
  public void testProvidedPartitioner() throws IOException {
    Schema user = SchemaBuilder.record("User").fields()
        .requiredString("username")
        .requiredString("email")
        .endRecord();
    PartitionStrategy strategy = new PartitionStrategy.Builder()
        .provided("version", "int")
        .build();
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(user)
        .partitionStrategy(strategy)
        .build();

    Path datasetPath = new Path("file:" + testDirectory + "/provided/users");

    final Dataset<GenericRecord> users = Datasets.create(
        "dataset:" + datasetPath, descriptor);

    final GenericRecord u1 = new GenericRecordBuilder(user)
        .set("username", "test1")
        .set("email", "a@example.com")
        .build();
    GenericRecord u2 = new GenericRecordBuilder(user)
        .set("username", "test2")
        .set("email", "b@example.com")
        .build();

    TestHelpers.assertThrows("Should reject write with unknown version",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            writeToView(users, u1);
          }
        });

    Assert.assertFalse(fileSystem.exists(new Path(datasetPath, "version=6")));
    writeToView(users.with("version", 6), u1);
    Assert.assertTrue(fileSystem.exists(new Path(datasetPath, "version=6")));

    Assert.assertFalse(fileSystem.exists(new Path(datasetPath, "version=7")));
    writeToView(Datasets.load("view:" + datasetPath + "?version=7"), u2);
    Assert.assertTrue(fileSystem.exists(new Path(datasetPath, "version=7")));

    Assert.assertEquals("Should read from provided partitions without view",
        Sets.newHashSet(u1, u2), DatasetTestUtilities.materialize(users));

    Assert.assertEquals("Should read from provided partition",
        Sets.newHashSet(u1),
        DatasetTestUtilities.materialize(users.with("version", 6)));

    Assert.assertEquals("Should read from provided partition",
        Sets.newHashSet(u2),
        DatasetTestUtilities.materialize(users.with("version", 7)));
  }

  private static <E> void writeToView(View<E> view, E... entities) {
    DatasetWriter<E> writer = null;
    try {
      writer = view.newWriter();
      for (E entity : entities) {
        writer.write(entity);
      }
      writer.close();
    } finally {
      if (writer != null) {
        writer.close();
      }
    }
  }

}
