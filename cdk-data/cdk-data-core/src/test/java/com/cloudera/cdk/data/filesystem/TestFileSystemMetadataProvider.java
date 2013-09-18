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

import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.Formats;
import com.cloudera.cdk.data.MetadataProvider;
import com.cloudera.cdk.data.PartitionStrategy;
import com.google.common.io.Files;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.cloudera.cdk.data.filesystem.DatasetTestUtilities.USER_SCHEMA;

public class TestFileSystemMetadataProvider {

  private Configuration conf;
  private FileSystem fileSystem;
  private Path testDirectory;

  @Before
  public void setUp() throws IOException {
    this.conf = new Configuration();
    this.fileSystem = FileSystem.get(conf);
    this.testDirectory = new Path(Files.createTempDir().getAbsolutePath());
  }

  @After
  public void tearDown() throws IOException {
    fileSystem.delete(testDirectory, true);
  }

  @Test
  public void testNonPartitioned() throws IOException {
    MetadataProvider provider = new FileSystemMetadataProvider(
        conf, testDirectory);

    provider.create("test", new DatasetDescriptor.Builder().schema(USER_SCHEMA).get());

    Assert.assertTrue("Descriptor properties file should exist", fileSystem
        .exists(new Path(testDirectory, "test/.metadata/descriptor.properties")));
    Assert.assertTrue("Descriptor schema file should exist",
        fileSystem.exists(new Path(testDirectory, "test/.metadata/schema.avsc")));

    DatasetDescriptor descriptor = provider.load("test");

    Assert.assertNotNull(descriptor);
    Assert.assertNotNull(descriptor.getSchema());
    Assert.assertEquals(Formats.AVRO, descriptor.getFormat());
    Assert.assertFalse(descriptor.isPartitioned());
    Assert.assertEquals("user", descriptor.getSchema().getName());
  }

  @Test
  public void testDelete() throws IOException {
    MetadataProvider provider = new FileSystemMetadataProvider(
        conf, testDirectory);

    provider.create("test", new DatasetDescriptor.Builder().schema(USER_SCHEMA).get());

    Assert.assertTrue("Descriptor properties file should exist", fileSystem
        .exists(new Path(testDirectory, "test/.metadata/descriptor.properties")));
    Assert.assertTrue("Descriptor schema file should exist",
        fileSystem.exists(new Path(testDirectory, "test/.metadata/schema.avsc")));

    boolean result = provider.delete("test");
    Assert.assertTrue(result);

    result = provider.delete("test");
    Assert.assertFalse(result);
  }

  @Test
  public void testPartitioned() throws IOException {
    MetadataProvider provider = new FileSystemMetadataProvider(
        conf, testDirectory);

    provider.create(
        "test",
        new DatasetDescriptor.Builder()
            .schema(USER_SCHEMA)
            .partitionStrategy(new PartitionStrategy.Builder().hash("username", 2).get())
            .get());

    Assert.assertTrue("Descriptor properties file should exist", fileSystem
        .exists(new Path(testDirectory, "test/.metadata/descriptor.properties")));
    Assert.assertTrue("Descriptor schema file should exist",
        fileSystem.exists(new Path(testDirectory, "test/.metadata/schema.avsc")));
  }

  @Test
  public void testNonDefaultFormat() throws IOException {
    MetadataProvider provider = new FileSystemMetadataProvider(
        conf, testDirectory);

    provider.create("test", new DatasetDescriptor.Builder().schema(USER_SCHEMA)
        .format(Formats.PARQUET).get());

    DatasetDescriptor descriptor = provider.load("test");

    Assert.assertNotNull(descriptor);
    Assert.assertEquals(Formats.PARQUET, descriptor.getFormat());
  }

}
