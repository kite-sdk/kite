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
package com.cloudera.cdk.data;

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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.net.URI;

public abstract class TestDatasetRepositories {

  protected static final String NAME = "test1";

  protected Configuration conf;
  protected FileSystem fileSystem;
  protected Path testDirectory;
  protected DatasetDescriptor testDescriptor;
  protected MetadataProvider testProvider;
  protected Schema testSchema;

  // from subclasses
  protected DatasetRepository repo;

  abstract public DatasetRepository newRepo(MetadataProvider provider);

  @Before
  public void setUp() throws IOException {
    this.testSchema = Schema.createRecord("Test", "Test record schema",
        "com.cloudera.cdk.data.filesystem", false);
    this.testSchema.setFields(Lists.newArrayList(new Field("name", Schema
        .create(Type.STRING), null, null)));

    this.conf = new Configuration();
    this.fileSystem = FileSystem.get(conf);
    this.testDirectory = new Path(Files.createTempDir().getAbsolutePath());
    this.testDescriptor = new DatasetDescriptor.Builder()
        .schema(testSchema)
        .get();
    this.testProvider = new MemoryMetadataProvider(conf) {
      @Override
      protected URI newLocation(String name) {
        // not used, but make sure its in the testDirectory to keep clean
        return new Path(testDirectory, name).toUri();
      }
    };

    this.repo = newRepo(testProvider);
  }

  @After
  public void tearDown() throws IOException {
    fileSystem.delete(testDirectory, true);
  }

  @Test
  public void testCreate() {
    Assert.assertFalse("Sanity check", testProvider.exists(NAME));

    Dataset dataset = repo.create(NAME, testDescriptor);
    Assert.assertNotNull("Dataset should be returned", dataset);
    Assert.assertTrue("Dataset should exist", repo.exists(NAME));

    DatasetDescriptor saved = testProvider.load(NAME);
    Assert.assertNotNull("Dataset metadata is stored under name", saved);
    Assert.assertEquals("Saved metadata is returned",
        saved, dataset.getDescriptor());

    Assert.assertEquals("Dataset name is propagated",
        NAME, dataset.getName());
    Assert.assertEquals("Dataset schema is propagated",
        testDescriptor.getSchema(), saved.getSchema());
  }

  public void ensureCreated() {
    // invoke the creation test so we have a dataset to test with.
    testCreate();
    Assert.assertTrue("Sanity check", testProvider.exists(NAME));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testCreateNullName() {
    repo.create(null, testDescriptor);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testCreateNullDescriptor() {
    repo.create(NAME, null);
  }

  @Test
  public void testCreatePartitioned() throws IOException {
    DatasetDescriptor requested = new DatasetDescriptor.Builder(testDescriptor)
        .partitionStrategy(
            new PartitionStrategy.Builder().hash("name", 3).get())
        .get();
    Assert.assertFalse("Sanity check", testProvider.exists("test2"));

    Dataset dataset = repo.create("test2", requested);

    DatasetDescriptor saved = testProvider.load("test2");
    Assert.assertNotNull("Dataset metadata is stored under name", saved);
    Assert.assertEquals("Saved metadata is returned",
        saved, dataset.getDescriptor());

    Assert.assertEquals("Dataset name is propagated",
        "test2", dataset.getName());
    Assert.assertEquals("Dataset schema is propagated",
        requested.getSchema(), saved.getSchema());
    Assert.assertEquals("Dataset partition strategy propagated",
        requested.getPartitionStrategy(), saved.getPartitionStrategy());
  }

  @Test(expected = DatasetExistsException.class)
  public void testCreateAlreadyExists() {
    ensureCreated();

    // create the same dataset again, this time it should fail
    repo.create(NAME, new DatasetDescriptor.Builder()
        .schema(testSchema).get());
  }

  @Test
  public void testUpdateSuccessWithoutChanges() {
    ensureCreated();

    try {
      repo.update(NAME, testProvider.load(NAME));
    } catch (UnsupportedOperationException ex) {
      // this is okay, too
    }
  }

  @Test(expected=NoSuchDatasetException.class)
  public void testUpdateNoDataset() {
    Assert.assertFalse("Sanity check", testProvider.exists(NAME));

    try {
      repo.update(NAME, testDescriptor);
    } catch (UnsupportedOperationException ex) {
      // this is okay, too
      throw new NoSuchDatasetException(ex);
    }
  }

  @Test(expected=IllegalArgumentException.class)
  public void testUpdateNullName() {
    ensureCreated();

    try {
      repo.update(null, testDescriptor);
    } catch (UnsupportedOperationException ex) {
      // this is okay, too
      throw new IllegalArgumentException();
    }
  }

  @Test(expected=IllegalArgumentException.class)
  public void testUpdateNullDescriptor() {
    ensureCreated();

    try {
      repo.update(NAME, null);
    } catch (UnsupportedOperationException ex) {
      // this is okay, too
      throw new IllegalArgumentException();
    }
  }

  @Test
  public void testList() {
    Assert.assertEquals(ImmutableMultiset.of(),
        ImmutableMultiset.copyOf(repo.list()));

    repo.create("test1", testDescriptor);
    Assert.assertEquals(ImmutableMultiset.of("test1"),
        ImmutableMultiset.copyOf(repo.list()));

    repo.create("test2", testDescriptor);
    Assert.assertEquals(ImmutableMultiset.of("test1", "test2"),
        ImmutableMultiset.copyOf(repo.list()));

    repo.create("test3", testDescriptor);
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

  @Test(expected=IllegalArgumentException.class)
  public void testExistsNullName() {
    repo.exists(null);
  }

  @Test
  public void testLoad() {
    ensureCreated();

    Dataset dataset = repo.load(NAME);

    Assert.assertNotNull("Dataset is loaded and produced", dataset);
    Assert.assertEquals("Dataset name is propagated",
        NAME, dataset.getName());
    Assert.assertEquals("Dataset schema is loaded",
        testSchema, dataset.getDescriptor().getSchema());
  }

  @Test(expected=NoSuchDatasetException.class)
  public void testLoadNoDataset() {
    Assert.assertFalse("Santity check", testProvider.exists(NAME));

    Dataset dataset = repo.load(NAME);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testLoadNullName() {
    repo.load(null);
  }

  @Test
  public void testDelete() {
    ensureCreated();

    boolean result = repo.delete(NAME);
    Assert.assertTrue("Delete dataset should return true", result);

    result = repo.delete(NAME);
    Assert.assertFalse("Delete nonexistent dataset should return false", result);
  }
}
