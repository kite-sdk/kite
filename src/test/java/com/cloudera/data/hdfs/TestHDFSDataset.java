package com.cloudera.data.hdfs;

import java.io.IOException;
import java.util.Set;

import com.cloudera.data.DatasetReader;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
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

import com.cloudera.data.DatasetWriter;
import com.cloudera.data.PartitionStrategy;
import com.google.common.io.Files;
import com.google.common.io.Resources;

public class TestHDFSDataset {

  private static final Logger logger = LoggerFactory
      .getLogger(TestHDFSDataset.class);

  private FileSystem fileSystem;
  private Path testDirectory;
  private Schema testSchema;
  private Schema testSchema2;

  @Before
  public void setUp() throws IOException {
    fileSystem = FileSystem.get(new Configuration());
    testDirectory = new Path(Files.createTempDir().getAbsolutePath());
    testSchema = new Schema.Parser().parse(Resources.getResource("user.avsc")
        .openStream());
    testSchema2 = new Schema.Parser().parse(Resources.getResource("user2.avsc")
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
        .set("username", "test").build();

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
      Assert.assertFalse(reader.hasNext());
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  @Test
  public void testPartitionedWriterSingle() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder()
        .hash("username", 2).get();

    HDFSDataset ds = new HDFSDataset.Builder().fileSystem(fileSystem)
        .directory(testDirectory).dataDirectory(testDirectory)
        .name("partitioned-users").schema(testSchema)
        .partitionStrategy(partitionStrategy).get();

    Assert.assertTrue("Dataset is partitioned", ds.isPartitioned());
    Assert.assertEquals(partitionStrategy, ds.getPartitionStrategy());

    writeTestUsers(ds, 10);
    readTestUsers(ds, 10);
  }

  @Test
  public void testPartitionedWriterDouble() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder()
        .hash("username", 2)
        .hash("email", 3)
        .get();

    HDFSDataset ds = new HDFSDataset.Builder().fileSystem(fileSystem)
        .directory(testDirectory).dataDirectory(testDirectory)
        .name("partitioned-users").schema(testSchema2)
        .partitionStrategy(partitionStrategy).get();

    Assert.assertTrue("Dataset is partitioned", ds.isPartitioned());
    Assert.assertEquals(partitionStrategy, ds.getPartitionStrategy());

    writeTestUsers2(ds, 10);
    readTestUsers2(ds, 10);
  }

  private void writeTestUsers(HDFSDataset ds, int count) throws IOException {
    DatasetWriter<Record> writer = null;

    try {
      writer = ds.getWriter();

      writer.open();

      for (int i = 0; i < count; i++) {
        Record record = new GenericRecordBuilder(testSchema).set("username",
            "test-" + i).build();

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

      // record order is not guaranteed, so check that we have read all the records
      Set<String> usernames = Sets.newHashSet();
      for (int i = 0; i < count; i++) {
        usernames.add("test-" + i);
      }
      for (int i = 0; i < count; i++) {
        Assert.assertTrue(reader.hasNext());
        Record actualRecord = reader.read();
        Assert.assertTrue(usernames.remove((String) actualRecord.get("username")));
      }
      Assert.assertTrue(usernames.isEmpty());
      Assert.assertFalse(reader.hasNext());
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  private void writeTestUsers2(HDFSDataset ds, int count) throws IOException {
    DatasetWriter<Record> writer = null;

    try {
      writer = ds.getWriter();

      writer.open();

      for (int i = 0; i < count; i++) {
        Record record = new GenericRecordBuilder(testSchema2).set("username",
            "test-" + i).set("email", "email-" + i).build();

        writer.write(record);
      }

      writer.flush();
    } finally {
      if (writer != null) {
        writer.close();
      }
    }
  }


  private void readTestUsers2(HDFSDataset ds, int count) throws IOException {
    DatasetReader<Record> reader = null;

    try {
      reader = ds.getReader();

      reader.open();

      // record order is not guaranteed, so check that we have read all the records
      Set<String> usernames = Sets.newHashSet();
      for (int i = 0; i < count; i++) {
        usernames.add("test-" + i);
      }
      for (int i = 0; i < count; i++) {
        Assert.assertTrue(reader.hasNext());
        Record actualRecord = reader.read();
        Assert.assertTrue(usernames.remove((String) actualRecord.get("username")));
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

}
