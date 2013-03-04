package com.cloudera.data.hdfs;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.data.DatasetDescriptor;
import com.cloudera.data.MetadataProvider;
import com.cloudera.data.PartitionExpression;
import com.google.common.io.Files;
import com.google.common.io.Resources;

public class TestFileSystemMetadataProvider {

  private FileSystem fileSystem;
  private Path testDirectory;

  @Before
  public void setUp() throws IOException {
    fileSystem = FileSystem.get(new Configuration());
    testDirectory = new Path(Files.createTempDir().getAbsolutePath());
  }

  @After
  public void tearDown() throws IOException {
    fileSystem.delete(testDirectory, true);
  }

  @Test
  public void testNonPartitioned() throws IOException {
    MetadataProvider provider = new FileSystemMetadataProvider(fileSystem,
        testDirectory);

    Schema userSchema = new Schema.Parser().parse(Resources.getResource(
        "schema/user.avsc").openStream());

    provider.save("test", new DatasetDescriptor.Builder().schema(userSchema)
        .get());

    Assert.assertTrue(fileSystem.exists(new Path(testDirectory,
        "test/descriptor.avro")));

    DatasetDescriptor descriptor = provider.load("test");

    Assert.assertNotNull(descriptor);
    Assert.assertNotNull(descriptor.getSchema());
    Assert.assertNull(descriptor.getPartitionStrategy());
    Assert.assertEquals("user", descriptor.getSchema().getName());
  }

  @Test
  public void testPartitioned() throws IOException {
    MetadataProvider provider = new FileSystemMetadataProvider(fileSystem,
        testDirectory);

    Schema userSchema = new Schema.Parser().parse(Resources.getResource(
        "schema/user-partitioned.avsc").openStream());

    provider.save(
        "test",
        new DatasetDescriptor.Builder()
            .schema(userSchema)
            .partitionStrategy(
                new PartitionExpression(userSchema
                    .getProp("cdk.partition.expression"), true).evaluate())
            .get());

    Assert.assertTrue(fileSystem.exists(new Path(testDirectory,
        "test/descriptor.avro")));
  }

}
