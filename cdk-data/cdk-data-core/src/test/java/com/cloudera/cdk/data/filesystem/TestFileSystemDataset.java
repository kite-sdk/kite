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

import com.cloudera.cdk.data.Dataset;
import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.DatasetException;
import com.cloudera.cdk.data.DatasetReader;
import com.cloudera.cdk.data.FieldPartitioner;
import com.cloudera.cdk.data.Format;
import com.cloudera.cdk.data.Formats;
import com.cloudera.cdk.data.Marker;
import com.cloudera.cdk.data.MiniDFSTest;
import com.cloudera.cdk.data.PartitionKey;
import com.cloudera.cdk.data.PartitionStrategy;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.cloudera.cdk.data.filesystem.DatasetTestUtilities.*;
import com.cloudera.cdk.data.impl.Accessor;

@RunWith(Parameterized.class)
public class TestFileSystemDataset extends MiniDFSTest {

  private static final Logger logger = LoggerFactory
    .getLogger(TestFileSystemDataset.class);

  @Parameterized.Parameters
  public static Collection<Object[]> data() throws IOException {
    MiniDFSTest.setupFS();
    Object[][] data = new Object[][] {
        { Formats.AVRO, getDFS() },
        { Formats.AVRO, getFS() },
        // Parquet fails when testing with HDFS because
        // parquet.hadoop.ParquetReader calls new Configuration(), which does
        // not work with the mini-cluster (not set up through env).
        //{ Formats.PARQUET, getDFS() },
        { Formats.PARQUET, getFS() } };
    return Arrays.asList(data);
  }

  private Format format;
  private FileSystem fileSystem;
  private Path testDirectory;
  private Marker username0;
  private Marker username1;

  public TestFileSystemDataset(Format format, FileSystem fs) {
    this.format = format;
    this.fileSystem = fs;
    this.username0 = new Marker.Builder("username", 0).get();
    this.username1 = new Marker.Builder("username", 1).get();
  }

  @Before
  public void setUp() throws IOException {
    testDirectory = fileSystem.makeQualified(
        new Path(Files.createTempDir().getAbsolutePath()));
  }

  @After
  public void tearDown() throws IOException {
    fileSystem.delete(testDirectory, true);
  }

  @Test
  public void testWriteAndRead() throws IOException {
    FileSystemDataset ds = new FileSystemDataset.Builder()
        .name("test")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schemaUri(USER_SCHEMA_URL)
            .format(format)
            .location(testDirectory)
            .get())
        .get();

    Assert.assertFalse("Dataset is not partitioned", ds.getDescriptor()
      .isPartitioned());

    writeTestUsers(ds, 10);
    checkTestUsers(ds, 10);
  }

  @Test
  public void testPartitionedWriterSingle() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder().hash(
      "username", 2).get();

    FileSystemDataset ds = new FileSystemDataset.Builder()
        .name("partitioned-users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .format(format)
            .location(testDirectory)
            .partitionStrategy(partitionStrategy)
            .get())
        .get();

    Assert.assertTrue("Dataset is partitioned", ds.getDescriptor()
      .isPartitioned());
    Assert.assertEquals(partitionStrategy, ds.getDescriptor()
      .getPartitionStrategy());

    writeTestUsers(ds, 10);
    Assert.assertTrue("Partitioned directory 0 exists",
      fileSystem.exists(new Path(testDirectory, "username=0")));
    Assert.assertTrue("Partitioned directory 1 exists",
      fileSystem.exists(new Path(testDirectory, "username=1")));
    checkTestUsers(ds, 10);
    PartitionKey key0 = partitionStrategy.keyFor(username0);
    PartitionKey key1 = partitionStrategy.keyFor(username1);
    int total = readTestUsersInPartition(ds, key0, null)
      + readTestUsersInPartition(ds, key1, null);
    Assert.assertEquals(10, total);

    testPartitionKeysAreEqual(ds, key0, key1);

    Set<Record> records = Sets.newHashSet();
    for (Dataset dataset : ds.getPartitions()) {
      Assert.assertFalse("Partitions should not have further partitions",
        dataset.getDescriptor().isPartitioned());
      records.addAll(materialize(ds));
    }
    checkTestUsers(records, 10);
  }

  @Test
  public void testPartitionedWriterDouble() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder()
      .hash("username", 2).hash("email", 3).get();

    FileSystemDataset ds = new FileSystemDataset.Builder()
        .name("partitioned-users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .format(format)
            .location(testDirectory)
            .partitionStrategy(partitionStrategy)
            .get())
        .get();

    Assert.assertTrue("Dataset is partitioned", ds.getDescriptor()
      .isPartitioned());
    Assert.assertEquals(partitionStrategy, ds.getDescriptor()
      .getPartitionStrategy());

    writeTestUsers(ds, 10);
    checkTestUsers(ds, 10);
    PartitionKey key0 = Accessor.getDefault().newPartitionKey(0);
    PartitionKey key1 = Accessor.getDefault().newPartitionKey(1);
    int total = readTestUsersInPartition(ds, key0, "email")
      + readTestUsersInPartition(ds, key0, "email");
    Assert.assertEquals(10, total);

    total = 0;
    for (int i1 = 0; i1 < 2; i1++) {
      for (int i2 = 0; i2 < 3; i2++) {
        String part = "username=" + i1 + "/email=" + i2;
        Assert.assertTrue("Partitioned directory " + part + " exists",
          fileSystem.exists(new Path(testDirectory, part)));
        Marker current = new Marker.Builder()
            .add("username", i1)
            .add("email", i2)
            .get();
        total += readTestUsersInPartition(ds,
          partitionStrategy.keyFor(current), null);
      }
    }
    Assert.assertEquals(10, total);

    testPartitionKeysAreEqual(ds, key0, key1);

    Set<Record> records = Sets.newHashSet();
    for (Dataset dataset : ds.getPartitions()) {
      Assert.assertTrue("Partitions should have further partitions", dataset
          .getDescriptor().isPartitioned());
      records.addAll(materialize(ds));
    }
    checkTestUsers(records, 10);

  }

  @Test
  public void testGetPartitionReturnsNullIfNoAutoCreate() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder().hash(
      "username", 2).get();

    FileSystemDataset ds = new FileSystemDataset.Builder()
        .name("partitioned-users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .format(format)
            .location(testDirectory)
            .partitionStrategy(partitionStrategy)
            .get())
        .get();

    Assert
      .assertNull(ds.getPartition(partitionStrategy.keyFor(username1), false));
  }

  @Test
  public void testWriteToSubpartition() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder()
      .hash("username", "username_part", 2).hash("email", 3).get();

    FileSystemDataset ds = new FileSystemDataset.Builder()
        .name("partitioned-users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .format(format)
            .location(testDirectory)
            .partitionStrategy(partitionStrategy)
            .get())
        .get();

    PartitionKey key = Accessor.getDefault().newPartitionKey(1);
    FileSystemDataset userPartition = (FileSystemDataset) ds.getPartition(key, true);
    Assert.assertEquals(key, userPartition.getPartitionKey());

    writeTestUsers(userPartition, 1);
    Assert.assertTrue("Partitioned directory exists",
      fileSystem.exists(new Path(testDirectory, "username_part=1/email=2")));
    Assert.assertEquals(1, readTestUsersInPartition(ds, key, "email"));
  }

  @Test
  public void testDropPartition() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder()
      .hash("username", 2).get();

    FileSystemDataset ds = new FileSystemDataset.Builder()
        .name("partitioned-users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .format(format)
            .location(testDirectory)
            .partitionStrategy(partitionStrategy)
            .get())
        .get();

    writeTestUsers(ds, 10);

    Assert.assertTrue(
      fileSystem.isDirectory(new Path(testDirectory, "username=0")));
    Assert.assertTrue(
      fileSystem.isDirectory(new Path(testDirectory, "username=1")));

    ds.dropPartition(partitionStrategy.keyFor(username0));
    Assert.assertFalse(
      fileSystem.isDirectory(new Path(testDirectory, "username=0")));

    ds.dropPartition(partitionStrategy.keyFor(username1));
    Assert.assertFalse(
      fileSystem.isDirectory(new Path(testDirectory, "username=1")));

    DatasetException caught = null;

    try {
      ds.dropPartition(partitionStrategy.keyFor(username0));
    } catch (DatasetException e) {
      caught = e;
    }

    Assert.assertNotNull(caught);
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
      reader = partition.newReader();
      reader.open();
      for (GenericData.Record actualRecord : reader) {
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
