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

import com.cloudera.data.Dataset;
import com.cloudera.data.DatasetDescriptor;
import com.cloudera.data.DatasetException;
import com.cloudera.data.DatasetReader;
import com.cloudera.data.DatasetWriter;
import com.cloudera.data.FieldPartitioner;
import com.cloudera.data.Format;
import com.cloudera.data.Formats;
import com.cloudera.data.PartitionKey;
import com.cloudera.data.PartitionStrategy;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import java.util.Arrays;
import java.util.Collection;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;

@RunWith(Parameterized.class)
public class TestFileSystemDataset {

  private static final Logger logger = LoggerFactory
    .getLogger(TestFileSystemDataset.class);

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] { { Formats.AVRO }, { Formats.PARQUET } };
    return Arrays.asList(data);
  }

  private Format format;
  private FileSystem fileSystem;
  private Path testDirectory;
  private Schema testSchema;

  public TestFileSystemDataset(Format format) {
    this.format = format;
  }

  @Before
  public void setUp() throws IOException {
    fileSystem = FileSystem.get(new Configuration());
    testDirectory = new Path(Files.createTempDir().getAbsolutePath());
    testSchema = new Schema.Parser().parse(Resources.getResource(
      "schema/user.avsc").openStream());
  }

  @After
  public void tearDown() throws IOException {
    fileSystem.delete(testDirectory, true);
  }

  @Test
  public void test() throws IOException {
    FileSystemDataset ds = new FileSystemDataset.Builder().name("test")
      .fileSystem(fileSystem).directory(testDirectory)
      .descriptor(new DatasetDescriptor.Builder().schema(testSchema)
          .format(format).get())
      .get();

    Schema schema = ds.getDescriptor().getSchema();
    Record record = new Record(schema);

    logger.debug("schema:{} record:{}", schema, record);

    record.put("username", "test");

    logger.debug("record:{}", record);
  }

  @Test
  public void testWriteAndRead() throws IOException {
    FileSystemDataset ds = new FileSystemDataset.Builder().name("test")
      .descriptor(new DatasetDescriptor.Builder().schema(testSchema)
          .format(format).get())
      .fileSystem(FileSystem.get(new Configuration()))
      .directory(testDirectory).get();

    logger.debug("Writing to dataset:{}", ds);

    Assert.assertFalse("Dataset is not partition", ds.getDescriptor()
      .isPartitioned());

    /*
     * Turns out ReflectDatumWriter subclasses GenericDatumWriter so this
     * actually works.
     */
    Record record = new GenericRecordBuilder(testSchema)
      .set("username", "test").set("email", "email-test").build();

    DatasetWriter<Record> writer = null;

    try {
      // TODO: Fix the cast situation. (Leave this warning until we do.)
      writer = ds.getWriter();

      writer.open();

      Assert.assertNotNull("Get writer produced a writer", writer);

      writer.write(record);
      writer.flush();
    } finally {
      if (writer != null) {
        writer.close();
      }
    }

    DatasetReader<Record> reader = null;
    try {
      reader = ds.getReader();
      reader.open();
      Assert.assertTrue(reader.hasNext());
      Record actualRecord = reader.read();
      Assert.assertEquals(record.get("username"), actualRecord.get("username"));
      Assert.assertEquals(record.get("email"), actualRecord.get("email"));
      Assert.assertFalse(reader.hasNext());
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  @Test
  public void testPartitionedWriterSingle() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder().hash(
      "username", 2).get();

    FileSystemDataset ds = new FileSystemDataset.Builder()
      .fileSystem(fileSystem)
      .directory(testDirectory)
      .name("partitioned-users")
      .descriptor(
        new DatasetDescriptor.Builder().schema(testSchema).format(format)
          .partitionStrategy(partitionStrategy).get()).get();

    Assert.assertTrue("Dataset is partitioned", ds.getDescriptor()
      .isPartitioned());
    Assert.assertEquals(partitionStrategy, ds.getDescriptor()
      .getPartitionStrategy());

    writeTestUsers(ds, 10);
    Assert.assertTrue("Partitioned directory 0 exists",
      fileSystem.exists(new Path(testDirectory, "username=0")));
    Assert.assertTrue("Partitioned directory 1 exists",
      fileSystem.exists(new Path(testDirectory, "username=1")));
    readTestUsers(ds, 10);
    int total = readTestUsersInPartition(ds, partitionStrategy.partitionKey(0),
      null)
      + readTestUsersInPartition(ds, partitionStrategy.partitionKey(1), null);
    Assert.assertEquals(10, total);

    Assert.assertEquals(2, Iterables.size(ds.getPartitions()));
    total = 0;
    for (Dataset dataset : ds.getPartitions()) {
      Assert.assertFalse("Partitions should not have further partitions",
        dataset.getDescriptor().isPartitioned());
      total += datasetSize(dataset);
    }
    Assert.assertEquals(10, total);

  }

  @Test
  public void testPartitionedWriterDouble() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder()
      .hash("username", 2).hash("email", 3).get();

    FileSystemDataset ds = new FileSystemDataset.Builder()
      .fileSystem(fileSystem)
      .directory(testDirectory)
      .name("partitioned-users")
      .descriptor(
        new DatasetDescriptor.Builder().schema(testSchema).format(format)
          .partitionStrategy(partitionStrategy).get()).get();

    Assert.assertTrue("Dataset is partitioned", ds.getDescriptor()
      .isPartitioned());
    Assert.assertEquals(partitionStrategy, ds.getDescriptor()
      .getPartitionStrategy());

    writeTestUsers(ds, 10);
    readTestUsers(ds, 10);
    int total = readTestUsersInPartition(ds, partitionStrategy.partitionKey(0),
      "email")
      + readTestUsersInPartition(ds, partitionStrategy.partitionKey(1),
      "email");
    Assert.assertEquals(10, total);

    total = 0;
    for (int i1 = 0; i1 < 2; i1++) {
      for (int i2 = 0; i2 < 3; i2++) {
        String part = "username=" + i1 + "/email=" + i2;
        Assert.assertTrue("Partitioned directory " + part + " exists",
          fileSystem.exists(new Path(testDirectory, part)));
        total += readTestUsersInPartition(ds,
          partitionStrategy.partitionKey(i1, i2), null);
      }
    }
    Assert.assertEquals(10, total);

    Assert.assertEquals(2, Iterables.size(ds.getPartitions()));
    total = 0;
    for (Dataset dataset : ds.getPartitions()) {
      Assert.assertTrue("Partitions should have further partitions", dataset
        .getDescriptor().isPartitioned());
      total += datasetSize(dataset);
    }
    Assert.assertEquals(10, total);

  }

  @Test
  public void testGetPartitionReturnsNullIfNoAutoCreate() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder().hash(
      "username", 2).get();

    FileSystemDataset ds = new FileSystemDataset.Builder()
      .fileSystem(fileSystem)
      .directory(testDirectory)
      .name("partitioned-users")
      .descriptor(
        new DatasetDescriptor.Builder().schema(testSchema).format(format)
          .partitionStrategy(partitionStrategy).get()).get();

    Assert
      .assertNull(ds.getPartition(partitionStrategy.partitionKey(1), false));
  }

  @Test
  public void testWriteToSubpartition() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder()
      .hash("username", 2).hash("email", 3).get();

    FileSystemDataset ds = new FileSystemDataset.Builder()
      .fileSystem(fileSystem)
      .directory(testDirectory)
      .name("partitioned-users")
      .descriptor(
        new DatasetDescriptor.Builder().schema(testSchema).format(format)
          .partitionStrategy(partitionStrategy).get()).get();

    Dataset userPartition = ds.getPartition(partitionStrategy.partitionKey(1),
      true);
    writeTestUsers(userPartition, 1);
    Assert.assertTrue("Partitioned directory exists",
      fileSystem.exists(new Path(testDirectory, "username=1/email=2")));
    Assert
      .assertEquals(
        1,
        readTestUsersInPartition(ds, partitionStrategy.partitionKey(1),
          "email"));
  }

  @Test
  public void testDropPartition() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder()
      .hash("username", 2).get();

    FileSystemDataset ds = new FileSystemDataset.Builder()
      .fileSystem(fileSystem)
      .directory(testDirectory)
      .name("partitioned-users")
      .descriptor(
        new DatasetDescriptor.Builder().schema(testSchema).format(format)
          .partitionStrategy(partitionStrategy).get()
      ).get();

    writeTestUsers(ds, 10);

    Assert.assertTrue(
      fileSystem.isDirectory(new Path(testDirectory, "username=0")));
    Assert.assertTrue(
      fileSystem.isDirectory(new Path(testDirectory, "username=1")));

    ds.dropPartition(partitionStrategy.partitionKey(0));
    Assert.assertFalse(
      fileSystem.isDirectory(new Path(testDirectory, "username=0")));

    ds.dropPartition(partitionStrategy.partitionKey(1));
    Assert.assertFalse(
      fileSystem.isDirectory(new Path(testDirectory, "username=1")));

    DatasetException caught = null;

    try {
      ds.dropPartition(partitionStrategy.partitionKey(0));
    } catch (DatasetException e) {
      caught = e;
    }

    Assert.assertNotNull(caught);
  }

  private int datasetSize(Dataset ds) {
    int size = 0;
    DatasetReader<Record> reader = null;
    try {
      reader = ds.getReader();
      reader.open();
      while (reader.hasNext()) {
        reader.read();
        size++;
      }
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
    return size;
  }

  private void writeTestUsers(Dataset ds, int count) {
    DatasetWriter<Record> writer = null;

    try {
      writer = ds.getWriter();

      writer.open();

      for (int i = 0; i < count; i++) {
        Record record = new GenericRecordBuilder(testSchema)
          .set("username", "test-" + i).set("email", "email-" + i).build();

        writer.write(record);
      }

      writer.flush();
    } finally {
      if (writer != null) {
        writer.close();
      }
    }
  }

  private void readTestUsers(Dataset ds, int count) {
    DatasetReader<Record> reader = null;

    try {
      reader = ds.getReader();

      reader.open();

      // record order is not guaranteed, so check that we have read all the
      // records
      Set<String> usernames = Sets.newHashSet();
      for (int i = 0; i < count; i++) {
        usernames.add("test-" + i);
      }
      for (int i = 0; i < count; i++) {
        Assert.assertTrue(reader.hasNext());
        Record actualRecord = reader.read();
        Assert.assertTrue(usernames.remove((String) actualRecord
          .get("username")));
        Assert.assertNotNull(actualRecord.get("email"));
      }
      Assert.assertTrue(usernames.isEmpty());
      Assert.assertFalse(reader.hasNext());
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  private int readTestUsersInPartition(FileSystemDataset ds, PartitionKey key,
    String subpartitionName) {
    int readCount = 0;
    DatasetReader<Record> reader = null;
    try {
      Dataset partition = ds.getPartition(key, false);
      if (subpartitionName != null) {
        List<FieldPartitioner> fieldPartitioners = partition.getDescriptor()
          .getPartitionStrategy().getFieldPartitioners();
        Assert.assertEquals(1, fieldPartitioners.size());
        Assert.assertEquals(subpartitionName, fieldPartitioners.get(0)
          .getName());
      }
      reader = partition.getReader();
      reader.open();
      while (reader.hasNext()) {
        Record actualRecord = reader.read();
        Assert.assertEquals(actualRecord.toString(), key.get(0), (actualRecord
          .get("username").hashCode() & Integer.MAX_VALUE) % 2);
        if (key.getLength() > 1) {
          Assert.assertEquals(key.get(1),
            (actualRecord.get("email").hashCode() & Integer.MAX_VALUE) % 3);
        }
        readCount++;
      }
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
    return readCount;
  }

}
