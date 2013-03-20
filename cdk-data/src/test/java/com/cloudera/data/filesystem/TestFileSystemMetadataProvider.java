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

import com.cloudera.data.DatasetDescriptor;
import com.cloudera.data.MetadataProvider;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

    Assert.assertTrue("Descriptor properties file should exist", fileSystem
        .exists(new Path(testDirectory, "test/.descriptor.properties")));
    Assert.assertTrue("Descriptor schema file should exist",
        fileSystem.exists(new Path(testDirectory, "test/.schema.avsc")));

    DatasetDescriptor descriptor = provider.load("test");

    Assert.assertNotNull(descriptor);
    Assert.assertNotNull(descriptor.getSchema());
    Assert.assertFalse(descriptor.isPartitioned());
    Assert.assertEquals("user", descriptor.getSchema().getName());
  }

  @Test
  public void testDelete() throws IOException {
    MetadataProvider provider = new FileSystemMetadataProvider(fileSystem,
        testDirectory);

    provider.save(
        "test",
        new DatasetDescriptor.Builder().schema(
            Resources.getResource("schema/user.avsc")).get());

    Assert.assertTrue("Descriptor properties file should exist", fileSystem
        .exists(new Path(testDirectory, "test/.descriptor.properties")));
    Assert.assertTrue("Descriptor schema file should exist",
        fileSystem.exists(new Path(testDirectory, "test/.schema.avsc")));

    boolean result = provider.delete("test");
    Assert.assertTrue(result);

    result = provider.delete("test");
    Assert.assertFalse(result);
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

    Assert.assertTrue("Descriptor properties file should exist", fileSystem
        .exists(new Path(testDirectory, "test/.descriptor.properties")));
    Assert.assertTrue("Descriptor schema file should exist",
        fileSystem.exists(new Path(testDirectory, "test/.schema.avsc")));
  }

}
