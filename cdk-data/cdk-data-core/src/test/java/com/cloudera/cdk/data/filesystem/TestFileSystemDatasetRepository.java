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
import com.cloudera.cdk.data.DatasetRepositoryException;
import com.cloudera.cdk.data.Formats;
import com.cloudera.cdk.data.PartitionStrategy;
import com.google.common.collect.Lists;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.io.Files;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.node.TextNode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.cloudera.cdk.data.filesystem.DatasetTestUtilities.datasetSize;

public class TestFileSystemDatasetRepository {

  private FileSystem fileSystem;
  private Path testDirectory;
  private FileSystemDatasetRepository repo;
  private Schema testSchema;

  @Before
  public void setUp() throws IOException {
    fileSystem = FileSystem.get(new Configuration());
    testDirectory = new Path(Files.createTempDir().getAbsolutePath());
    repo = new FileSystemDatasetRepository(fileSystem, testDirectory);

    testSchema = Schema.createRecord("Test", "Test record schema",
        "com.cloudera.cdk.data.filesystem", false);
    testSchema.setFields(Lists.newArrayList(new Field("name", Schema
        .create(Type.STRING), null, null)));
  }

  @After
  public void tearDown() throws IOException {
    fileSystem.delete(testDirectory, true);
  }

  @Test
  public void testCreate() throws IOException {
    Dataset dataset = repo.create("test1", new DatasetDescriptor.Builder()
        .schema(testSchema).get());

    Assert.assertEquals("Dataset name is propagated", "test1",
        dataset.getName());
    Assert.assertEquals("Dataset schema is propagated", testSchema, dataset
        .getDescriptor().getSchema());
    Assert.assertTrue("Dataset data directory exists",
        fileSystem.exists(new Path(testDirectory, "test1/")));
    Assert.assertTrue("Dataset properties file exists", fileSystem
        .exists(new Path(testDirectory, "test1/.metadata/descriptor.properties")));
    Assert.assertTrue("Dataset schema file exists",
        fileSystem.exists(new Path(testDirectory, "test1/.metadata/schema.avsc")));
  }

  @Test
  public void testList() {
    Assert.assertEquals(ImmutableMultiset.of(),
        ImmutableMultiset.copyOf(repo.list()));

    repo.create("test1", new DatasetDescriptor.Builder()
        .schema(testSchema).get());
    Assert.assertEquals(ImmutableMultiset.of("test1"),
        ImmutableMultiset.copyOf(repo.list()));

    repo.create("test2", new DatasetDescriptor.Builder()
        .schema(testSchema).get());
    Assert.assertEquals(ImmutableMultiset.of("test1", "test2"),
        ImmutableMultiset.copyOf(repo.list()));

    repo.create("test3", new DatasetDescriptor.Builder()
        .schema(testSchema).get());
    Assert.assertEquals(ImmutableMultiset.of("test1", "test2", "test3"),
        ImmutableMultiset.copyOf(repo.list()));

    repo.delete("test2");
    Assert.assertEquals(ImmutableMultiset.of("test1", "test3"),
        ImmutableMultiset.copyOf(repo.list()));

    repo.delete("test3");
    Assert.assertEquals(ImmutableMultiset.of("test1"),
        ImmutableMultiset.copyOf(repo.list()));

    repo.delete("test1");
    Assert.assertEquals(ImmutableMultiset.of(),
        ImmutableMultiset.copyOf(repo.list()));
  }

  @Test
  public void testExists() {
    Assert.assertFalse(repo.exists("test1"));

    repo.create("test1", new DatasetDescriptor.Builder()
        .schema(testSchema).get());
    Assert.assertTrue(repo.exists("test1"));

    repo.delete("test1");
    Assert.assertFalse(repo.exists("test1"));
  }

  @Test
  public void testCreatePartitioned() throws IOException {
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(testSchema)
        .partitionStrategy(
            new PartitionStrategy.Builder().hash("name", 3).get()).get();

    Dataset dataset = repo.create("test2", descriptor);

    Assert.assertEquals("Dataset name is propagated", "test2",
        dataset.getName());
    Assert.assertEquals("Dataset schema is propagated", testSchema, dataset
        .getDescriptor().getSchema());
    Assert.assertTrue("Dataset data directory exists",
        fileSystem.exists(new Path(testDirectory, "test2/")));
    Assert.assertTrue("Dataset properties file exists", fileSystem
        .exists(new Path(testDirectory, "test2/.metadata/descriptor.properties")));
    Assert.assertTrue("Dataset schema file exists",
        fileSystem.exists(new Path(testDirectory, "test2/.metadata/schema.avsc")));
  }

  @Test
  public void testLoad() throws IOException {
    // Just invoke the creation test so we have a dataset to test with.
    testCreate();

    Dataset dataset = repo.load("test1");

    Assert.assertNotNull("Dataset is loaded and produced", dataset);
    Assert.assertEquals("Dataset name is propagated", "test1",
        dataset.getName());
    Assert.assertEquals("Dataset schema is loaded", testSchema, dataset
        .getDescriptor().getSchema());

    /*
     * We perform a read test to make sure only data files are encountered
     * during a read.
     */
    Assert.assertEquals(0, datasetSize(dataset));
  }

  @Test
  public void testDelete() throws IOException {
    // Just invoke the creation test so we have a dataset to test with.
    testCreate();

    boolean result = repo.delete("test1");
    Assert.assertTrue("Delete dataset should return true", result);

    result = repo.delete("test1");
    Assert.assertFalse("Delete nonexistent dataset should return false", result);
  }

  @Test
  public void testUpdateFailsWithFormatChange() throws IOException {
    Dataset dataset = repo.create("test1", new DatasetDescriptor.Builder()
        .schema(testSchema).format(Formats.AVRO).get());
    try {
      repo.update("test1", new DatasetDescriptor.Builder().schema(testSchema).format
          (Formats.PARQUET).get());
      Assert.fail("Should fail due to format change");
    } catch (DatasetRepositoryException e) {
      // expected
    }
    Assert.assertEquals(Formats.AVRO, repo.load("test1").getDescriptor().getFormat());
  }

  @Test
  public void testUpdateFailsWithPartitionStrategyChange() throws IOException {
    PartitionStrategy ps = new PartitionStrategy.Builder()
        .hash("username", 2).get();
    Dataset dataset = repo.create("test1", new DatasetDescriptor.Builder()
        .schema(testSchema).partitionStrategy(ps).get());
    try {
      repo.update("test1", new DatasetDescriptor.Builder()
          .schema(testSchema).partitionStrategy(new PartitionStrategy.Builder()
              .hash("username", 2).hash("email", 3).get()).get());
      Assert.fail("Should fail due to partition strategy change");
    } catch (DatasetRepositoryException e) {
      // expected
    }
    Assert.assertEquals(ps, repo.load("test1").getDescriptor().getPartitionStrategy());
  }

  @Test
  public void testUpdate() throws IOException {
    Dataset dataset = repo.create("test1", new DatasetDescriptor.Builder()
        .schema(testSchema).get());

    Assert.assertEquals("Dataset name is propagated", "test1",
        dataset.getName());
    Assert.assertEquals("Dataset schema is propagated", testSchema, dataset
        .getDescriptor().getSchema());

    Schema testSchemaV2 = Schema.createRecord("Test", "Test record schema",
        "com.cloudera.cdk.data.filesystem", false);
    testSchemaV2.setFields(Lists.newArrayList(
        new Field("name", Schema.create(Type.STRING), null, null),
        new Field("email", Schema.create(Type.STRING), null, null) // incompatible - no default
    ));

    try {
      repo.update("test1", new DatasetDescriptor.Builder().schema(testSchemaV2).get());
      Assert.fail("Should fail due to incompatible update");
    } catch (DatasetRepositoryException e) {
      // expected
    }
    dataset = repo.load("test1");
    Assert.assertEquals("Dataset schema is unchanged", testSchema, dataset
        .getDescriptor().getSchema());

    Schema testSchemaV3 = Schema.createRecord("Test", "Test record schema",
        "com.cloudera.cdk.data.filesystem", false);
    testSchemaV3.setFields(Lists.newArrayList(
        new Field("name", Schema.create(Type.STRING), null, null),
        new Field("email", Schema.create(Type.STRING), null,
            TextNode.valueOf("a@example.com"))
    ));

    Dataset datasetV3 = repo.update("test1", new DatasetDescriptor.Builder().schema
        (testSchemaV3).get());

    Assert.assertEquals("Dataset schema is updated", testSchemaV3, datasetV3
        .getDescriptor().getSchema());
  }

}
