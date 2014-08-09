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

import com.google.common.collect.Lists;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.ValidationException;
import org.kitesdk.data.spi.PartitionKey;
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
import org.kitesdk.data.CompressionType;
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.spi.PartitionedDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.kitesdk.data.spi.filesystem.DatasetTestUtilities.*;
import org.kitesdk.data.spi.FieldPartitioner;

@RunWith(Parameterized.class)
public class TestFileSystemDataset extends MiniDFSTest {

  private static final Logger LOG = LoggerFactory
    .getLogger(TestFileSystemDataset.class);

  @Parameterized.Parameters
  public static Collection<Object[]> data() throws IOException {
    MiniDFSTest.setupFS();
    Object[][] data = new Object[][] {
        { Formats.AVRO, getDFS(), CompressionType.Snappy },
        { Formats.AVRO, getDFS(), CompressionType.Deflate},
        { Formats.AVRO, getDFS(), CompressionType.Bzip2},
        { Formats.AVRO, getFS(), CompressionType.Snappy},
        { Formats.AVRO, getFS(), CompressionType.Deflate},
        { Formats.AVRO, getFS(), CompressionType.Bzip2},
        { Formats.PARQUET, getDFS(), CompressionType.Snappy },
        { Formats.PARQUET, getDFS(), CompressionType.Deflate },
        { Formats.PARQUET, getFS(), CompressionType.Snappy },
        { Formats.PARQUET, getFS(), CompressionType.Deflate } };
    return Arrays.asList(data);
  }

  private final Format format;
  private final FileSystem fileSystem;
  private final CompressionType compressionType;
  private Path testDirectory;

  public TestFileSystemDataset(Format format, FileSystem fs,
      CompressionType compressionType) {
    this.format = format;
    this.fileSystem = fs;
    this.compressionType = compressionType;
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
    FileSystemDataset<Record> ds = new FileSystemDataset.Builder<Record>()
        .namespace("ns")
        .name("test")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schemaUri(USER_SCHEMA_URL)
            .format(format)
            .compressionType(compressionType)
            .location(testDirectory)
            .build())
        .type(Record.class)
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

    FileSystemDataset<Record> ds = new FileSystemDataset.Builder<Record>()
        .namespace("ns")
        .name("partitioned-users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .format(format)
            .compressionType(compressionType)
            .location(testDirectory)
            .partitionStrategy(partitionStrategy)
            .build())
        .type(Record.class)
        .build();

    Assert.assertTrue("Dataset is partitioned", ds.getDescriptor()
      .isPartitioned());
    Assert.assertEquals(partitionStrategy, ds.getDescriptor()
      .getPartitionStrategy());

    writeTestUsers(ds, 10);
    Assert.assertTrue("Partitioned directory 0 exists",
      fileSystem.exists(new Path(testDirectory, "username_hash=0")));
    Assert.assertTrue("Partitioned directory 1 exists",
      fileSystem.exists(new Path(testDirectory, "username_hash=1")));
    checkTestUsers(ds, 10);
    PartitionKey key0 = new PartitionKey(0);
    PartitionKey key1 = new PartitionKey(1);
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

    FileSystemDataset<Record> ds = new FileSystemDataset.Builder<Record>()
        .namespace("ns")
        .name("partitioned-users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .format(format)
            .compressionType(compressionType)
            .location(testDirectory)
            .partitionStrategy(partitionStrategy)
            .build())
        .type(Record.class)
        .build();

    Assert.assertTrue("Dataset is partitioned", ds.getDescriptor()
      .isPartitioned());
    Assert.assertEquals(partitionStrategy, ds.getDescriptor()
      .getPartitionStrategy());

    writeTestUsers(ds, 10);
    checkTestUsers(ds, 10);
    PartitionKey key0 = new PartitionKey(0);
    PartitionKey key1 = new PartitionKey(1);
    int total = readTestUsersInPartition(ds, key0, "email_hash")
      + readTestUsersInPartition(ds, key0, "email_hash");
    Assert.assertEquals(10, total);

    total = 0;
    for (int i1 = 0; i1 < 2; i1++) {
      for (int i2 = 0; i2 < 3; i2++) {
        String part = "username_hash=" + i1 + "/email_hash=" + i2;
        Assert.assertTrue("Partitioned directory " + part + " exists",
          fileSystem.exists(new Path(testDirectory, part)));
        total += readTestUsersInPartition(ds,
          new PartitionKey(i1, i2), null);
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

    FileSystemDataset<Record> ds = new FileSystemDataset.Builder<Record>()
        .namespace("ns")
        .name("partitioned-users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .format(format)
            .location(testDirectory)
            .partitionStrategy(partitionStrategy)
            .build())
        .type(Record.class)
        .build();

    Assert
      .assertNull(ds.getPartition(new PartitionKey(1), false));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testWriteToSubpartition() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder()
      .hash("username", "username_part", 2).hash("email", 3).build();

    FileSystemDataset<Record> ds = new FileSystemDataset.Builder<Record>()
        .namespace("ns")
        .name("partitioned-users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .format(format)
            .compressionType(compressionType)
            .location(testDirectory)
            .partitionStrategy(partitionStrategy)
            .build())
        .type(Record.class)
        .build();

    PartitionKey key = new PartitionKey(1);
    FileSystemDataset<Record> userPartition = (FileSystemDataset<Record>) ds.getPartition(key, true);
    Assert.assertEquals(key, userPartition.getPartitionKey());

    writeTestUsers(userPartition, 1);
    Assert.assertTrue("Partitioned directory exists",
      fileSystem.exists(new Path(testDirectory, "username_part=1/email_hash=2")));
    Assert.assertEquals(1, readTestUsersInPartition(ds, key, "email_hash"));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testDropPartition() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder()
      .hash("username", 2).build();

    FileSystemDataset<Record> ds = new FileSystemDataset.Builder<Record>()
        .namespace("ns")
        .name("partitioned-users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .format(format)
            .compressionType(compressionType)
            .location(testDirectory)
            .partitionStrategy(partitionStrategy)
            .build())
        .type(Record.class)
        .build();

    writeTestUsers(ds, 10);

    Assert.assertTrue(
      fileSystem.isDirectory(new Path(testDirectory, "username_hash=0")));
    Assert.assertTrue(
      fileSystem.isDirectory(new Path(testDirectory, "username_hash=1")));

    ds.dropPartition(new PartitionKey(0));
    Assert.assertFalse(
      fileSystem.isDirectory(new Path(testDirectory, "username_hash=0")));

    ds.dropPartition(new PartitionKey(1));
    Assert.assertFalse(
      fileSystem.isDirectory(new Path(testDirectory, "username_hash=1")));

    DatasetException caught = null;

    try {
      ds.dropPartition(new PartitionKey(0));
    } catch (DatasetException e) {
      caught = e;
    }

    Assert.assertNotNull(caught);
  }
  
  @Test
  public void testMerge() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder().hash(
        "username", 2).build();

    FileSystemDataset<Record> ds = new FileSystemDataset.Builder<Record>()
        .namespace("ns")
        .name("partitioned-users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .format(format)
            .compressionType(compressionType)
            .location(testDirectory)
            .partitionStrategy(partitionStrategy)
            .build())
        .type(Record.class)
        .build();

    writeTestUsers(ds, 10);
    checkTestUsers(ds, 10);

    Path newTestDirectory = fileSystem.makeQualified(
        new Path(Files.createTempDir().getAbsolutePath()));

    FileSystemDataset<Record> dsUpdate = new FileSystemDataset.Builder<Record>()
        .namespace("ns")
        .name("partitioned-users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .format(format)
            .compressionType(compressionType)
            .location(newTestDirectory)
            .partitionStrategy(partitionStrategy)
            .build())
        .type(Record.class)
        .build();

    writeTestUsers(dsUpdate, 5, 10);
    checkTestUsers(dsUpdate, 5, 10);

    ds.merge(dsUpdate);

    checkTestUsers(dsUpdate, 0);
    checkTestUsers(ds, 15);

  }

  @Test(expected = ValidationException.class)
  public void testCannotMergeDatasetsWithDifferentFormats() throws IOException {
    FileSystemDataset<Record> ds = new FileSystemDataset.Builder<Record>()
        .namespace("ns")
        .name("users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .format(Formats.AVRO)
            .location(testDirectory)
            .build())
        .type(Record.class)
        .build();
    FileSystemDataset<Record> dsUpdate = new FileSystemDataset.Builder<Record>()
        .namespace("ns")
        .name("users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .format(Formats.PARQUET)
            .location(testDirectory)
            .build())
        .type(Record.class)
        .build();
    ds.merge(dsUpdate);
  }

  @Test(expected = ValidationException.class)
  public void testCannotMergeDatasetsWithDifferentPartitionStrategies() throws IOException {
    FileSystemDataset<Record> ds = new FileSystemDataset.Builder<Record>()
        .namespace("ns")
        .name("users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .location(testDirectory)
            .partitionStrategy(new PartitionStrategy.Builder()
                .hash("username", 2).build())
            .build())
        .type(Record.class)
        .build();
    FileSystemDataset<Record> dsUpdate = new FileSystemDataset.Builder<Record>()
        .namespace("ns")
        .name("users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .location(testDirectory)
            .partitionStrategy(new PartitionStrategy.Builder()
                .hash("username", 2).hash("email", 3).build())
            .build())
        .type(Record.class)
        .build();
    ds.merge(dsUpdate);
  }

  @Test(expected = ValidationException.class)
  public void testCannotMergeDatasetsWithDifferentSchemas() throws IOException {
    FileSystemDataset<Record> ds = new FileSystemDataset.Builder<Record>()
        .namespace("ns")
        .name("users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(STRING_SCHEMA)
            .location(testDirectory)
            .build())
        .type(Record.class)
        .build();
    FileSystemDataset<Record> dsUpdate = new FileSystemDataset.Builder<Record>()
        .namespace("ns")
        .name("users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .location(testDirectory)
            .build())
        .type(Record.class)
        .build();
    ds.merge(dsUpdate);
  }

  @Test
  public void testPathIterator_Directory() {
    FileSystemDataset<Record> ds = new FileSystemDataset.Builder<Record>()
        .namespace("ns")
        .name("users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .format(format)
            .compressionType(compressionType)
            .location(testDirectory)
            .build())
        .type(Record.class)
        .build();

    List<Path> dirPaths = Lists.newArrayList(ds.dirIterator());
    Assert.assertEquals("dirIterator for non-partitioned dataset should yield a single path.", 1, dirPaths.size());
    Assert.assertEquals("dirIterator should yield absolute paths.", testDirectory, dirPaths.get(0));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testPathIterator_Partition_Directory() {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder()
        .hash("username", 2).hash("email", 3).build();

    final FileSystemDataset<Record> ds = new FileSystemDataset.Builder<Record>()
        .namespace("ns")
        .name("partitioned-users")
        .configuration(getConfiguration())
        .descriptor(new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .format(format)
            .compressionType(compressionType)
            .location(testDirectory)
            .partitionStrategy(partitionStrategy)
            .build())
        .type(Record.class)
        .build();

    Assert.assertTrue("Dataset is partitioned", ds.getDescriptor()
        .isPartitioned());
    Assert.assertEquals(partitionStrategy, ds.getDescriptor()
        .getPartitionStrategy());

    writeTestUsers(ds, 10);
    checkTestUsers(ds, 10);

    List<Path> dirPaths = Lists.newArrayList(ds.dirIterator());

    // 2 user directories * 3 email directories
    Assert.assertEquals(6, dirPaths.size());

    Assert.assertTrue("dirIterator should yield absolute paths.", dirPaths.get(0).isAbsolute());

    FileSystemDataset<Record> partition = (FileSystemDataset<Record>)
        ds.getPartition(new PartitionKey(1, 2), false);
    List<Path> leafPaths = Lists.newArrayList(partition.dirIterator());
    Assert.assertEquals(1, leafPaths.size());
    final Path leafPath = leafPaths.get(0);
    Assert.assertTrue("dirIterator should yield absolute paths.", leafPath.isAbsolute());

    Assert.assertEquals(new PartitionKey(1, 2), ds.keyFromDirectory(leafPath));
    Assert.assertEquals(new PartitionKey(1), ds.keyFromDirectory(leafPath.getParent()));
    Assert.assertEquals(new PartitionKey(), ds.keyFromDirectory(leafPath.getParent().getParent()));

    TestHelpers.assertThrows("Path with too many components",
        IllegalStateException.class, new Runnable() {
      @Override
      public void run() {
        ds.keyFromDirectory(new Path(leafPath, "extra_dir"));
      }
    });

    TestHelpers.assertThrows("Non-relative path",
        IllegalStateException.class, new Runnable() {
      @Override
      public void run() {
        ds.keyFromDirectory(new Path("hdfs://different_host/"));
      }
    });
  }
  
  @Test
  public void testDeleteAllWithoutPartitions() {
    final FileSystemDataset<Record> ds = new FileSystemDataset.Builder<Record>()
        .namespace("ns")
        .name("users")
        .configuration(getConfiguration())
        .descriptor(
            new DatasetDescriptor.Builder().schema(USER_SCHEMA).format(format)
                .location(testDirectory).build())
        .type(Record.class)
        .build();
    
    writeTestUsers(ds, 10);
    
    Assert.assertTrue(ds.deleteAll());
    
    checkReaderBehavior(ds.newReader(), 0, (RecordValidator<Record>) null);
  }

  @SuppressWarnings("deprecation")
  private int readTestUsersInPartition(FileSystemDataset<Record> ds, PartitionKey key,
      String subpartitionName) {
    int readCount = 0;
    DatasetReader<Record> reader = null;
    try {
      PartitionedDataset<Record> partition = ds.getPartition(key, false);
      if (subpartitionName != null) {
        List<FieldPartitioner> fieldPartitioners = partition.getDescriptor()
            .getPartitionStrategy().getFieldPartitioners();
        Assert.assertEquals(1, fieldPartitioners.size());
        Assert.assertEquals(subpartitionName, fieldPartitioners.get(0)
            .getName());
      }
      reader = partition.newReader();
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
