package com.cloudera.data.hdfs;

import java.io.File;
import java.io.IOException;

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

import com.cloudera.data.PartitionExpression;
import com.google.common.io.Closeables;
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
    testSchema = new Schema.Parser().parse(Resources.getResource("user.avsc")
        .openStream());
  }

  @After
  public void tearDown() throws IOException {
    fileSystem.delete(testDirectory, true);
  }

  @Test
  public void test() throws IOException {
    HDFSDataset ds = new HDFSDataset();

    ds.setSchema(new Schema.Parser().parse(new File(Resources.getResource(
        "user.avsc").getPath())));

    Schema schema = ds.getSchema();
    Record record = new Record(schema);

    logger.debug("schema:{} record:{}", schema, record);

    record.put("username", "test");

    logger.debug("record:{}", record);
  }

  @Test
  public void testGetWriter() throws IOException {
    HDFSDataset ds = new HDFSDataset.Builder().name("test").schema(testSchema)
        .fileSystem(FileSystem.get(new Configuration()))
        .dataDirectory(testDirectory).get();

    logger.debug("Writing to dataset:{}", ds);

    Assert.assertFalse("Dataset is not partition", ds.isPartitioned());

    /*
     * Turns out ReflectDatumWriter subclasses GenericDatumWriter so this
     * actually works.
     */
    Record record = new GenericRecordBuilder(testSchema)
        .set("username", "test").build();

    HDFSDatasetWriter<Record> writer = null;

    try {
      // TODO: Fix the cast situation. (Leave this warning until we do.)
      writer = ds.getWriter();

      writer.open();

      Assert.assertNotNull("Get writer produced a writer", writer);

      writer.write(record);
      writer.flush();
    } finally {
      Closeables.close(writer, false);
    }
  }

  @Test
  public void testPartitionedWriter() throws IOException {
    PartitionExpression expression = new PartitionExpression(
        "[record.username.hashCode() % 2]");

    HDFSDataset ds = new HDFSDataset.Builder().fileSystem(fileSystem)
        .dataDirectory(testDirectory).name("partitioned-users")
        .schema(testSchema).partitionExpression(expression).get();

    Assert.assertTrue("Dataset is partitioned", ds.isPartitioned());
    Assert.assertEquals(expression, ds.getPartitionExpression());

    Record record = new GenericRecordBuilder(testSchema)
        .set("username", "test").build();
    HDFSDatasetWriter<Record> writer = null;

    try {
      writer = ds.getWriter();

      writer.open();

      writer.write(record);
      writer.flush();
    } finally {
      Closeables.close(writer, false);
    }
  }

}
