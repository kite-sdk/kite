package com.cloudera.data.hdfs;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class TestHDFSDatasetRepository {

  private FileSystem fileSystem;
  private Path testDirectory;
  private HDFSDatasetRepository repo;
  private Schema testSchema;

  @Before
  public void setUp() throws IOException {
    Configuration conf = new Configuration();

    conf.set("fs.default.name", "file:///");

    fileSystem = FileSystem.get(conf);
    testDirectory = new Path(Files.createTempDir().getAbsolutePath());
    repo = new HDFSDatasetRepository();

    repo.setFileSystem(fileSystem);
    repo.setRootDirectory(new Path(testDirectory, "data"));

    testSchema = Schema.createRecord("Test", "Test record schema",
        "com.cloudera.data.hdfs", false);
    testSchema.setFields(Lists.newArrayList(new Field("name", Schema
        .create(Type.STRING), null, null)));
  }

  @After
  public void tearDown() throws IOException {
    fileSystem.delete(testDirectory, true);
  }

  @Test
  public void testCreate() throws IOException {
    HDFSDataset dataset = repo.create("test1", testSchema);

    Assert.assertEquals("Dataset name is propagated", "test1",
        dataset.getName());
    Assert.assertEquals("Dataset schema is propagated", testSchema,
        dataset.getSchema());
    Assert.assertTrue("Dataset data directory exists",
        fileSystem.exists(new Path(testDirectory, "data/test1/data")));
    Assert.assertTrue("Dataset metadata file exists",
        fileSystem.exists(new Path(testDirectory, "data/test1/schema.avsc")));

    Schema schema = dataset.getSchema();
    Schema serializedSchema = new Schema.Parser().parse(new File(testDirectory
        .toUri().getPath(), "data/test1/schema.avsc"));

    Assert.assertEquals("Dataset schema matches what's serialized to disk",
        schema, serializedSchema);
  }

  @Test
  public void testLoad() throws IOException {
    // Just invoke the creation test so we have a dataset to test with.
    testCreate();

    HDFSDataset dataset = repo.get("test1");

    Assert.assertNotNull("Dataset is loaded and produced", dataset);
    Assert.assertEquals("Dataset name is propagated", "test1",
        dataset.getName());
    Assert.assertEquals("Dataset schema is loaded", testSchema,
        dataset.getSchema());
  }

}
