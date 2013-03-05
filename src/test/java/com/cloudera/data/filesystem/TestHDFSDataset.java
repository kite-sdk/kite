package com.cloudera.data.filesystem;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.data.Dataset;
import com.cloudera.data.DatasetReader;
import com.cloudera.data.DatasetWriter;
import com.cloudera.data.FieldPartitioner;
import com.cloudera.data.PartitionKey;
import com.cloudera.data.PartitionStrategy;
import com.cloudera.data.filesystem.HDFSDataset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.io.Resources;

public class TestHDFSDataset {

  private static final Logger logger = LoggerFactory
      .getLogger(TestHDFSDataset.class);

  private FileSystem fileSystem;
  private Path testDirectory;
  private Schema testSchema;

  @Before
  public void setUp() throws IOException {
    fileSystem = FileSystem.get(new Configuration());
    testDirectory = new Path(Files.createTempDir().getAbsolutePath());
    testSchema = new Schema.Parser().parse(Resources.getResource("schema/user.avsc")
        .openStream());
  }

  @After
  public void tearDown() throws IOException {
    fileSystem.delete(testDirectory, true);
  }

  @Test
  public void test() throws IOException {
    HDFSDataset ds = new HDFSDataset();

    ds.setSchema(testSchema);

    Schema schema = ds.getSchema();
    Record record = new Record(schema);

    logger.debug("schema:{} record:{}", schema, record);

    record.put("username", "test");

    logger.debug("record:{}", record);
  }

  @Test
  public void testWriteAndRead() throws IOException {
    HDFSDataset ds = new HDFSDataset.Builder().name("test").schema(testSchema)
        .fileSystem(FileSystem.get(new Configuration()))
        .directory(testDirectory).dataDirectory(testDirectory).get();

    logger.debug("Writing to dataset:{}", ds);

    Assert.assertFalse("Dataset is not partition", ds.isPartitioned());

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

    HDFSDataset ds = new HDFSDataset.Builder().fileSystem(fileSystem)
        .directory(testDirectory).dataDirectory(testDirectory)
        .name("partitioned-users").schema(testSchema)
        .partitionStrategy(partitionStrategy).get();

    Assert.assertTrue("Dataset is partitioned", ds.isPartitioned());
    Assert.assertEquals(partitionStrategy, ds.getPartitionStrategy());

    writeTestUsers(ds, 10);
    readTestUsers(ds, 10);
    int total = readTestUsersInPartition(ds, new PartitionKey(0), null)
        + readTestUsersInPartition(ds, new PartitionKey(1), null);
    Assert.assertEquals(10, total);

    Assert.assertEquals(2, Iterables.size(ds.getPartitions()));
    total = 0;
    for (Dataset dataset : ds.getPartitions()) {
      Assert.assertFalse("Partitions should not have further partitions",
          dataset.isPartitioned());
      total += datasetSize(dataset);
    }
    Assert.assertEquals(10, total);

  }

  @Test
  public void testPartitionedWriterDouble() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder()
        .hash("username", 2).hash("email", 3).get();

    HDFSDataset ds = new HDFSDataset.Builder().fileSystem(fileSystem)
        .directory(testDirectory).dataDirectory(testDirectory)
        .name("partitioned-users").schema(testSchema)
        .partitionStrategy(partitionStrategy).get();

    Assert.assertTrue("Dataset is partitioned", ds.isPartitioned());
    Assert.assertEquals(partitionStrategy, ds.getPartitionStrategy());

    writeTestUsers(ds, 10);
    readTestUsers(ds, 10);
    int total = readTestUsersInPartition(ds, new PartitionKey(0), "email")
        + readTestUsersInPartition(ds, new PartitionKey(1), "email");
    Assert.assertEquals(10, total);

    total = 0;
    for (int i1 = 0; i1 < 2; i1++) {
      for (int i2 = 0; i2 < 3; i2++) {
        total += readTestUsersInPartition(ds, new PartitionKey(i1, i2), null);
      }
    }
    Assert.assertEquals(10, total);

    Assert.assertEquals(6, Iterables.size(ds.getPartitions()));
    total = 0;
    for (Dataset dataset : ds.getPartitions()) {
      Assert.assertFalse("Partitions should not have further partitions",
          dataset.isPartitioned());
      total += datasetSize(dataset);
    }
    Assert.assertEquals(10, total);

  }

  private int datasetSize(Dataset ds) throws IOException {
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

  private void writeTestUsers(HDFSDataset ds, int count) throws IOException {
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

  private void readTestUsers(HDFSDataset ds, int count) throws IOException {
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

  private int readTestUsersInPartition(HDFSDataset ds, PartitionKey key,
      String subpartitionName) throws IOException {
    int readCount = 0;
    DatasetReader<Record> reader = null;
    try {
      Dataset partition = ds.getPartition(key, false);
      if (subpartitionName != null) {
        List<FieldPartitioner> fieldPartitioners = partition
            .getPartitionStrategy().getFieldPartitioners();
        Assert.assertEquals(1, fieldPartitioners.size());
        Assert.assertEquals(subpartitionName, fieldPartitioners.get(0)
            .getName());
      }
      reader = partition.getReader();
      reader.open();
      while (reader.hasNext()) {
        Record actualRecord = reader.read();
        Assert.assertEquals(key.getValues().get(0),
            (actualRecord.get("username").hashCode() & Integer.MAX_VALUE) % 2);
        if (key.getLength() > 1) {
          Assert.assertEquals(key.getValues().get(1), (actualRecord
              .get("email").hashCode() & Integer.MAX_VALUE) % 3);
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
