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

import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetRepositoryException;
import org.kitesdk.data.FieldPartitioner;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.PartitionKey;
import org.kitesdk.data.PartitionStrategy;
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

import static org.kitesdk.data.filesystem.DatasetTestUtilities.*;
import org.kitesdk.data.impl.Accessor;

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

  public TestFileSystemDataset(Format format, FileSystem fs) {
    this.format = format;
    this.fileSystem = fs;
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
    FileSystemDataset<Record> ds = new FileSystemDataset.Builder()
        .name("test")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schemaUri(USER_SCHEMA_URL)
            .format(format)
            .location(testDirectory)
            .build())
        .build();

    Assert.assertFalse("Dataset is not partitioned", ds.getDescriptor()
      .isPartitioned());

    writeTestUsers(ds, 10);
    checkTestUsers(ds, 10);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testPartitionedWriterSingle() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder().hash(
      "username", 2).build();

    FileSystemDataset<Record> ds = new FileSystemDataset.Builder()
        .name("partitioned-users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .format(format)
            .location(testDirectory)
            .partitionStrategy(partitionStrategy)
            .build())
        .build();

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
    PartitionKey key0 = partitionStrategy.partitionKey(0);
    PartitionKey key1 = partitionStrategy.partitionKey(1);
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
  @SuppressWarnings("deprecation")
  public void testPartitionedWriterDouble() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder()
      .hash("username", 2).hash("email", 3).build();

    FileSystemDataset<Record> ds = new FileSystemDataset.Builder()
        .name("partitioned-users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .format(format)
            .location(testDirectory)
            .partitionStrategy(partitionStrategy)
            .build())
        .build();

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
        total += readTestUsersInPartition(ds,
          partitionStrategy.partitionKey(i1, i2), null);
      }
    }
    Assert.assertEquals(10, total);

    testPartitionKeysAreEqual(ds, key0, key1);

    Set<Record> records = Sets.newHashSet();
    for (Dataset<Record> dataset : ds.getPartitions()) {
      Assert.assertTrue("Partitions should have further partitions", dataset
          .getDescriptor().isPartitioned());
      records.addAll(materialize(ds));
    }
    checkTestUsers(records, 10);

  }

  @Test
  @SuppressWarnings("deprecation")
  public void testGetPartitionReturnsNullIfNoAutoCreate() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder().hash(
      "username", 2).build();

    FileSystemDataset<Record> ds = new FileSystemDataset.Builder()
        .name("partitioned-users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .format(format)
            .location(testDirectory)
            .partitionStrategy(partitionStrategy)
            .build())
        .build();

    Assert
      .assertNull(ds.getPartition(partitionStrategy.partitionKey(1), false));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testWriteToSubpartition() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder()
      .hash("username", "username_part", 2).hash("email", 3).build();

    FileSystemDataset<Record> ds = new FileSystemDataset.Builder()
        .name("partitioned-users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .format(format)
            .location(testDirectory)
            .partitionStrategy(partitionStrategy)
            .build())
        .build();

    PartitionKey key = Accessor.getDefault().newPartitionKey(1);
    FileSystemDataset<Record> userPartition = (FileSystemDataset<Record>) ds.getPartition(key, true);
    Assert.assertEquals(key, userPartition.getPartitionKey());

    writeTestUsers(userPartition, 1);
    Assert.assertTrue("Partitioned directory exists",
      fileSystem.exists(new Path(testDirectory, "username_part=1/email=2")));
    Assert.assertEquals(1, readTestUsersInPartition(ds, key, "email"));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testDropPartition() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder()
      .hash("username", 2).build();

    FileSystemDataset<Record> ds = new FileSystemDataset.Builder()
        .name("partitioned-users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .format(format)
            .location(testDirectory)
            .partitionStrategy(partitionStrategy)
            .build())
        .build();

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
  
  @Test
  public void testMerge() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder().hash(
        "username", 2).build();

    FileSystemDataset<Record> ds = new FileSystemDataset.Builder()
        .name("partitioned-users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .format(format)
            .location(testDirectory)
            .partitionStrategy(partitionStrategy)
            .build())
        .build();

    writeTestUsers(ds, 10);
    checkTestUsers(ds, 10);

    Path newTestDirectory = fileSystem.makeQualified(
        new Path(Files.createTempDir().getAbsolutePath()));

    FileSystemDataset<Record> dsUpdate = new FileSystemDataset.Builder()
        .name("partitioned-users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .format(format)
            .location(newTestDirectory)
            .partitionStrategy(partitionStrategy)
            .build())
        .build();

    writeTestUsers(dsUpdate, 5, 10);
    checkTestUsers(dsUpdate, 5, 10);

    ds.merge(dsUpdate);

    checkTestUsers(dsUpdate, 0);
    checkTestUsers(ds, 15);

  }

  @Test(expected = DatasetRepositoryException.class)
  public void testCannotMergeDatasetsWithDifferentFormats() throws IOException {
    FileSystemDataset<Record> ds = new FileSystemDataset.Builder()
        .name("users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .format(Formats.AVRO)
            .location(testDirectory)
            .build())
        .build();
    FileSystemDataset<Record> dsUpdate = new FileSystemDataset.Builder()
        .name("users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .format(Formats.PARQUET)
            .location(testDirectory)
            .build())
        .build();
    ds.merge(dsUpdate);
  }

  @Test(expected = DatasetRepositoryException.class)
  public void testCannotMergeDatasetsWithDifferentPartitionStrategies() throws IOException {
    FileSystemDataset<Record> ds = new FileSystemDataset.Builder()
        .name("users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .location(testDirectory)
            .partitionStrategy(new PartitionStrategy.Builder()
                .hash("username", 2).build())
            .build())
        .build();
    FileSystemDataset<Record> dsUpdate = new FileSystemDataset.Builder()
        .name("users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .location(testDirectory)
            .partitionStrategy(new PartitionStrategy.Builder()
                .hash("username", 2).hash("email", 3).build())
            .build())
        .build();
    ds.merge(dsUpdate);
  }

  @Test(expected = DatasetRepositoryException.class)
  public void testCannotMergeDatasetsWithDifferentSchemas() throws IOException {
    FileSystemDataset<Record> ds = new FileSystemDataset.Builder()
        .name("users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(STRING_SCHEMA)
            .location(testDirectory)
            .build())
        .build();
    FileSystemDataset<Record> dsUpdate = new FileSystemDataset.Builder()
        .name("users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .location(testDirectory)
            .build())
        .build();
    ds.merge(dsUpdate);
  }

  @SuppressWarnings("deprecation")
  private int readTestUsersInPartition(FileSystemDataset<Record> ds, PartitionKey key,
      String subpartitionName) {
    int readCount = 0;
    DatasetReader<Record> reader = null;
    try {
      Dataset<Record> partition = ds.getPartition(key, false);
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
