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
package org.kitesdk.data;

import org.kitesdk.data.spi.filesystem.DatasetTestUtilities;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.io.Files;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.MemoryMetadataProvider;
import org.kitesdk.data.spi.MetadataProvider;

@RunWith(Parameterized.class)
public abstract class TestDatasetRepositories extends MiniDFSTest {

  protected static final String NAMESPACE = "ns1";
  protected static final String NAME = "test1";

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] {
        { false },  // default to local FS
        { true } }; // default to distributed FS
    return Arrays.asList(data);
  }

  // whether this should use the DFS provided by MiniDFSTest
  private boolean distributed;

  protected Configuration conf;
  protected FileSystem fileSystem;
  protected Path testDirectory;
  protected DatasetDescriptor testDescriptor;
  protected MetadataProvider testProvider;
  protected Schema testSchema;

  // from subclasses
  protected DatasetRepository repo;

  abstract public DatasetRepository newRepo(MetadataProvider provider);

  public MetadataProvider newProvider(Configuration conf) {
    return new MemoryMetadataProvider(conf) {
      @Override
      protected URI newLocation(String name) {
        // not used, but make sure its in the testDirectory to keep clean
        return new Path(testDirectory, name).toUri();
      }
    };
  }

  public TestDatasetRepositories(boolean distributed) {
    this.distributed = distributed;
  }

  @Before
  public void setUp() throws IOException {
    this.testSchema = DatasetTestUtilities.USER_SCHEMA;

    Configuration tempConfig = new Configuration();
    tempConfig.setLong("fs.trash.interval", 1);

    this.conf = (distributed ?
        MiniDFSTest.getConfiguration() : tempConfig);

    this.fileSystem = FileSystem.get(conf);
    this.testDirectory = fileSystem.makeQualified(
        new Path(Files.createTempDir().getAbsolutePath()));
    this.testDescriptor = new DatasetDescriptor.Builder()
        .schema(testSchema)
        .build();

    this.testProvider = newProvider(conf);
    this.repo = newRepo(testProvider);
  }

  @After
  public void tearDown() throws IOException {
    fileSystem.delete(testDirectory, true);
  }

  @Test
  public void testCreate() {
    Assert.assertFalse("Sanity check", testProvider.exists(NAMESPACE, NAME));

    Dataset dataset = repo.create(NAMESPACE, NAME, testDescriptor);
    Assert.assertNotNull("Dataset should be returned", dataset);
    Assert.assertTrue("Dataset should exist", repo.exists(NAMESPACE, NAME));

    DatasetDescriptor saved = testProvider.load(NAMESPACE, NAME);
    Assert.assertNotNull("Dataset metadata is stored under name", saved);
    Assert.assertEquals("Saved metadata is returned",
        saved, dataset.getDescriptor());

    // TODO: Add test for namespace accessor
    Assert.assertEquals("Dataset name is propagated",
        NAME, dataset.getName());
    Assert.assertEquals("Dataset schema is propagated",
        testDescriptor.getSchema(), saved.getSchema());
    Assert.assertNotNull("Dataset should have a URI location",
        saved.getLocation());
    Assert.assertNotNull("Dataset location should have a scheme",
        saved.getLocation().getScheme());
  }

  public void ensureCreated() {
    // invoke the creation test so we have a dataset to test with.
    testCreate();
    Assert.assertTrue("Sanity check", testProvider.exists(NAMESPACE, NAME));
  }

  @Test(expected=NullPointerException.class)
  public void testCreateNullNamespace() {
    repo.create(null, NAME, testDescriptor);
  }

  @Test(expected=NullPointerException.class)
  public void testCreateNullName() {
    repo.create(NAMESPACE, null, testDescriptor);
  }

  @Test(expected=NullPointerException.class)
  public void testCreateNullDescriptor() {
    repo.create(NAMESPACE, NAME, null);
  }

  @Test
  public void testCreatePartitioned() throws IOException {
    DatasetDescriptor requested = new DatasetDescriptor.Builder(testDescriptor)
        .partitionStrategy(
            new PartitionStrategy.Builder().hash("username", 3).build())
        .build();
    Assert.assertFalse("Sanity check", testProvider.exists(NAMESPACE, "test2"));

    Dataset dataset = repo.create(NAMESPACE, "test2", requested);

    DatasetDescriptor saved = testProvider.load(NAMESPACE, "test2");
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
    repo.create(NAMESPACE, NAME, new DatasetDescriptor.Builder()
        .schema(testSchema).build());
  }

  @Test
  public void testUpdateSuccessWithoutChanges() {
    ensureCreated();
    repo.update(NAMESPACE, NAME, testProvider.load(NAMESPACE, NAME));
  }

  @Test(expected=DatasetNotFoundException.class)
  public void testUpdateNoDataset() {
    Assert.assertFalse("Sanity check", testProvider.exists(NAMESPACE, NAME));

    repo.update(NAMESPACE, NAME, testDescriptor);
  }

  @Test(expected=NullPointerException.class)
  public void testUpdateNullNamespace() {
    ensureCreated();

    repo.update(null, NAME, testDescriptor);
  }

  @Test(expected=NullPointerException.class)
  public void testUpdateNullName() {
    ensureCreated();

    repo.update(NAMESPACE, null, testDescriptor);
  }

  @Test(expected=NullPointerException.class)
  public void testUpdateNullDescriptor() {
    ensureCreated();

    repo.update(NAMESPACE, NAME, null);
  }

  @Test
  public void testListDatasets() {
    Assert.assertEquals(ImmutableMultiset.<String>of(),
        ImmutableMultiset.copyOf(repo.datasets(NAMESPACE)));

    repo.create(NAMESPACE, "test1", testDescriptor);
    Assert.assertEquals(ImmutableMultiset.of("test1"),
        ImmutableMultiset.copyOf(repo.datasets(NAMESPACE)));

    repo.create(NAMESPACE, "test2", testDescriptor);
    Assert.assertEquals(ImmutableMultiset.of("test1", "test2"),
        ImmutableMultiset.copyOf(repo.datasets(NAMESPACE)));

    repo.create(NAMESPACE, "test3", testDescriptor);
    Assert.assertEquals(ImmutableMultiset.of("test1", "test2", "test3"),
        ImmutableMultiset.copyOf(repo.datasets(NAMESPACE)));

    repo.delete(NAMESPACE, "test2");
    Assert.assertEquals(ImmutableMultiset.of("test1", "test3"),
        ImmutableMultiset.copyOf(repo.datasets(NAMESPACE)));

    repo.delete(NAMESPACE, "test3");
    Assert.assertEquals(ImmutableMultiset.of("test1"),
        ImmutableMultiset.copyOf(repo.datasets(NAMESPACE)));

    repo.delete(NAMESPACE, "test1");
    Assert.assertEquals(ImmutableMultiset.<String>of(),
        ImmutableMultiset.copyOf(repo.datasets(NAMESPACE)));
  }

  @Test
  public void testListNamespaces() {
    DatasetDescriptor anotherDescriptor = new DatasetDescriptor
        .Builder(testDescriptor)
        .property("prop", "value")
        .build();

    Assert.assertEquals(ImmutableMultiset.<String>of(),
        ImmutableMultiset.copyOf(repo.namespaces()));

    repo.create("test1", "d1", testDescriptor);
    repo.create("test1", "d2", anotherDescriptor);
    Assert.assertEquals(ImmutableMultiset.of("test1"),
        ImmutableMultiset.copyOf(repo.namespaces()));

    repo.create("test2", "d1", testDescriptor);
    repo.create("test2", "d2", anotherDescriptor);
    Assert.assertEquals(ImmutableMultiset.of("test1", "test2"),
        ImmutableMultiset.copyOf(repo.namespaces()));

    repo.create("test3", "d1", testDescriptor);
    repo.create("test3", "d2", anotherDescriptor);
    Assert.assertEquals(ImmutableMultiset.of("test1", "test2", "test3"),
        ImmutableMultiset.copyOf(repo.namespaces()));

    repo.delete("test2", "d2");
    Assert.assertEquals(ImmutableMultiset.of("test1", "test2", "test3"),
        ImmutableMultiset.copyOf(repo.namespaces()));

    repo.delete("test2", "d1");
    Assert.assertEquals(ImmutableMultiset.of("test1", "test3"),
        ImmutableMultiset.copyOf(repo.namespaces()));

    repo.delete("test3", "d2");
    Assert.assertEquals(ImmutableMultiset.of("test1", "test3"),
        ImmutableMultiset.copyOf(repo.namespaces()));

    repo.delete("test3", "d1");
    Assert.assertEquals(ImmutableMultiset.of("test1"),
        ImmutableMultiset.copyOf(repo.namespaces()));

    repo.delete("test1", "d1");
    Assert.assertEquals(ImmutableMultiset.of("test1"),
        ImmutableMultiset.copyOf(repo.namespaces()));

    repo.delete("test1", "d2");
    Assert.assertEquals(ImmutableMultiset.<String>of(),
        ImmutableMultiset.copyOf(repo.namespaces()));
  }

  @Test
  public void testExists() {
    Assert.assertFalse(repo.exists(NAMESPACE, "test1"));

    repo.create(NAMESPACE, "test1", new DatasetDescriptor.Builder()
        .schema(testSchema).build());
    Assert.assertTrue(repo.exists(NAMESPACE, "test1"));

    repo.delete(NAMESPACE, "test1");
    Assert.assertFalse(repo.exists(NAMESPACE, "test1"));
  }

  @Test(expected=NullPointerException.class)
  public void testExistsNullNamespace() {
    repo.exists(null, NAME);
  }

  @Test(expected=NullPointerException.class)
  public void testExistsNullName() {
    repo.exists(NAMESPACE, null);
  }

  @Test
  public void testLoad() {
    ensureCreated();

    Dataset dataset = repo.load(NAMESPACE, NAME);

    Assert.assertNotNull("Dataset is loaded and produced", dataset);
    Assert.assertEquals("Dataset name is propagated",
        NAME, dataset.getName());
    Assert.assertEquals("Dataset schema is loaded",
        testSchema, dataset.getDescriptor().getSchema());
  }

  @Test(expected=DatasetNotFoundException.class)
  public void testLoadNoDataset() {
    Assert.assertFalse("Santity check", testProvider.exists(NAMESPACE, NAME));

    Dataset dataset = repo.load(NAMESPACE, NAME);
  }

  @Test(expected=NullPointerException.class)
  public void testLoadNullNamespace() {
    repo.load(null, NAME);
  }

  @Test(expected=NullPointerException.class)
  public void testLoadNullName() {
    repo.load(NAMESPACE, (String) null);
  }

  @Test
  public void testDelete() {
    ensureCreated();

    boolean result = repo.delete(NAMESPACE, NAME);
    Assert.assertTrue("Delete dataset should return true", result);

    result = repo.delete(NAMESPACE, NAME);
    Assert.assertFalse("Delete nonexistent dataset should return false", result);
  }

  @Test
  public void testMoveToTrash() {
    ensureCreated();

    boolean result = repo.moveToTrash(NAMESPACE, NAME);
    Assert.assertTrue("Delete dataset should return true", result);

    result = repo.moveToTrash(NAMESPACE, NAME);
    Assert.assertFalse("Delete nonexistent dataset should return false", result);
  }
}
