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
package org.kitesdk.data.spi;

import com.google.common.collect.ImmutableMultiset;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetExistsException;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.Formats;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.PartitionStrategy;

@RunWith(Parameterized.class)
public abstract class TestMetadataProviders extends MiniDFSTest {

  protected static final String NAMESPACE = "ns1";
  protected static final String NAME = "provider_test1";

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] {
        { false },  // default to local FS
        { true } }; // default to distributed FS
    return Arrays.asList(data);
  }

  // whether this should use the DFS provided by MiniDFSTest
  protected boolean distributed;

  protected Configuration conf;
  protected DatasetDescriptor testDescriptor;
  protected DatasetDescriptor anotherDescriptor;

  protected MetadataProvider provider;
  abstract public MetadataProvider newProvider(Configuration conf);

  public TestMetadataProviders(boolean distributed) {
    this.distributed = distributed;
  }

  @Before
  public void setUp() throws IOException, URISyntaxException {
    this.conf = (distributed ?
        MiniDFSTest.getConfiguration() :
        new Configuration());
    this.testDescriptor = new DatasetDescriptor.Builder()
        .format(Formats.AVRO)
        .schema(SchemaBuilder.record("Event").fields()
            .requiredLong("timestamp")
            .requiredString("message")
            .endRecord())
        .partitionStrategy(new PartitionStrategy.Builder()
            .year("timestamp")
            .month("timestamp")
            .day("timestamp")
            .build())
        .build();
    // something completely different
    this.anotherDescriptor = new DatasetDescriptor.Builder()
        .format(Formats.PARQUET)
        .schema(SchemaBuilder.record("Record").fields()
            .requiredBytes("some_field")
            .requiredString("another_field")
            .endRecord())
        .partitionStrategy(new PartitionStrategy.Builder()
            .hash("some_field", 20000)
            .build())
        .build();

    this.provider = newProvider(conf);
  }

  @Test
  public void testCreate() {
    Assert.assertFalse("Sanity check", provider.exists(NAMESPACE, NAME));

    DatasetDescriptor created = provider.create(NAMESPACE, NAME, testDescriptor);

    Assert.assertNotNull("Descriptor should be returned", created);
    Assert.assertTrue("Descriptor should exist", provider.exists(NAMESPACE, NAME));
    Assert.assertEquals("Schema should match",
        testDescriptor.getSchema(), created.getSchema());
    Assert.assertEquals("PartitionStrategy should match",
        testDescriptor.getPartitionStrategy(), created.getPartitionStrategy());
    Assert.assertEquals("Format should match",
        testDescriptor.getFormat(), created.getFormat());
    // the MetadataProvider optionally sets the location, nothing to test
  }

  @Test
  public void testCreateWithLocation() throws URISyntaxException {
    Assert.assertFalse("Sanity check", provider.exists(NAMESPACE, NAME));

    String auth = getDFS().getUri().getAuthority();
    URI requestedLocation = new URI("hdfs://" + auth + "/tmp/data/my_data_set");
    DatasetDescriptor requested = new DatasetDescriptor.Builder(testDescriptor)
        .location(requestedLocation)
        .build();

    final DatasetDescriptor created;
    try {
      created = provider.create(NAMESPACE, NAME, requested);
    } catch (UnsupportedOperationException ex) {
      // this is expected if the provider doesn't support requested locations
      return;
    }

    // if supported, the location should be unchanged.
    Assert.assertNotNull("Descriptor should be returned", created);
    Assert.assertTrue("Descriptor should exist", provider.exists(NAMESPACE, NAME));
    Assert.assertEquals("Requested locations should match",
        requestedLocation, created.getLocation());
  }

  public void ensureCreated() {
    // use testCreate to create NAMESPACE/NAME
    testCreate();
    Assert.assertTrue("Sanity check", provider.exists(NAMESPACE, NAME));
  }

  @Test(expected=DatasetExistsException.class)
  public void testCreateAlreadyExists() {
    ensureCreated();
    provider.create(NAMESPACE, NAME, anotherDescriptor);
  }

  @Test(expected=NullPointerException.class)
  public void testCreateFailsNullNamespace() {
    provider.create(null, NAME, testDescriptor);
  }

  @Test(expected=NullPointerException.class)
  public void testCreateFailsNullName() {
    provider.create(NAMESPACE, null, testDescriptor);
  }

  @Test(expected=NullPointerException.class)
  public void testCreateFailsNullDescriptor() {
    provider.create(NAMESPACE, NAME, null);
  }

  @Test
  public void testLoad() {
    ensureCreated();

    DatasetDescriptor loaded = provider.load(NAMESPACE, NAME);

    Assert.assertNotNull("DatasetDescriptor should be returned", loaded);
    Assert.assertEquals("Schema should match",
        testDescriptor.getSchema(), loaded.getSchema());
    Assert.assertEquals("PartitionStrategy should match",
        testDescriptor.getPartitionStrategy(), loaded.getPartitionStrategy());
    Assert.assertEquals("Format should match",
        testDescriptor.getFormat(), loaded.getFormat());
  }

  @Test(expected=DatasetNotFoundException.class)
  public void testLoadNoDataset() {
    Assert.assertFalse("Sanity check", provider.exists(NAMESPACE, NAME));
    provider.load(NAMESPACE, NAME);
  }

  @Test(expected=NullPointerException.class)
  public void testLoadFailsNullNamespace() {
    provider.load(null, NAME);
  }

  @Test(expected=NullPointerException.class)
  public void testLoadFailsNullName() {
    provider.load(NAMESPACE, null);
  }

  public void testUpdate() {
    ensureCreated();

    /*
     * To be clear: we are testing that even crazy, incompatible changes are
     * happily saved by the MetadataProvider. Rule enforcement is done upstream
     * by libraries that are in a better position to make decisions about what
     * changes are incompatible.
     */

    final DatasetDescriptor saved = provider.update(NAMESPACE, NAME, anotherDescriptor);

    Assert.assertNotNull("Updated Descriptor should be returned", saved);
    Assert.assertEquals("Schema should match update",
        anotherDescriptor.getSchema(), saved.getSchema());
    Assert.assertEquals("PartitionStrategy should match update",
        anotherDescriptor.getPartitionStrategy(), saved.getPartitionStrategy());
    Assert.assertEquals("Format should match update",
        anotherDescriptor.getFormat(), saved.getFormat());
  }

  @Test(expected=DatasetNotFoundException.class)
  public void testUpdateFailsNoDataset() {
    provider.update(NAMESPACE, NAME, testDescriptor);
  }

  @Test(expected=NullPointerException.class)
  public void testUpdateFailsNullNamespace() {
    provider.update(null, NAME, testDescriptor);
  }

  @Test(expected=NullPointerException.class)
  public void testUpdateFailsNullName() {
    provider.update(NAMESPACE, null, testDescriptor);
  }

  @Test(expected=NullPointerException.class)
  public void testUpdateFailsNullDescriptor() {
    provider.update(NAMESPACE, NAME, null);
  }

  public void testDelete() {
    ensureCreated();

    boolean result = provider.delete(NAMESPACE, NAME);
    Assert.assertTrue("Delete descriptor should return true", result);

    result = provider.delete(NAMESPACE, NAME);
    Assert.assertFalse("Delete non-existent descriptor should return false", result);
  }

  @Test(expected=NullPointerException.class)
  public void testDeleteFailsNullNamespace() {
    provider.delete(null, NAME);
  }

  @Test(expected=NullPointerException.class)
  public void testDeleteFailsNullName() {
    provider.delete(NAMESPACE, null);
  }

  @Test
  public void testExists() {
    Assert.assertFalse(provider.exists(NAMESPACE, NAME));

    provider.create(NAMESPACE, NAME, testDescriptor);
    Assert.assertTrue(provider.exists(NAMESPACE, NAME));

    provider.delete(NAMESPACE, NAME);
    Assert.assertFalse(provider.exists(NAMESPACE, NAME));
  }

  @Test(expected=NullPointerException.class)
  public void testExistsNullNamespace() {
    provider.exists(null, NAME);
  }

  @Test(expected=NullPointerException.class)
  public void testExistsNullName() {
    provider.exists(NAMESPACE, null);
  }

  @Test
  public void testListDatasets() {
    Assert.assertEquals(ImmutableMultiset.<String>of(),
        ImmutableMultiset.copyOf(provider.datasets(NAMESPACE)));

    provider.create(NAMESPACE, "test1", testDescriptor);
    Assert.assertEquals(ImmutableMultiset.of("test1"),
        ImmutableMultiset.copyOf(provider.datasets(NAMESPACE)));

    provider.create(NAMESPACE, "test2", testDescriptor);
    Assert.assertEquals(ImmutableMultiset.of("test1", "test2"),
        ImmutableMultiset.copyOf(provider.datasets(NAMESPACE)));

    provider.create(NAMESPACE, "test3", testDescriptor);
    Assert.assertEquals(ImmutableMultiset.of("test1", "test2", "test3"),
        ImmutableMultiset.copyOf(provider.datasets(NAMESPACE)));

    provider.delete(NAMESPACE, "test2");
    Assert.assertEquals(ImmutableMultiset.of("test1", "test3"),
        ImmutableMultiset.copyOf(provider.datasets(NAMESPACE)));

    provider.delete(NAMESPACE, "test3");
    Assert.assertEquals(ImmutableMultiset.of("test1"),
        ImmutableMultiset.copyOf(provider.datasets(NAMESPACE)));

    provider.delete(NAMESPACE, "test1");
    Assert.assertEquals(ImmutableMultiset.<String>of(),
        ImmutableMultiset.copyOf(provider.datasets(NAMESPACE)));
  }

  @Test
  public void testListNamespaces() {
    Assert.assertEquals(ImmutableMultiset.<String>of(),
        ImmutableMultiset.copyOf(provider.namespaces()));

    provider.create("test1", "d1", testDescriptor);
    provider.create("test1", "d2", anotherDescriptor);
    Assert.assertEquals(ImmutableMultiset.of("test1"),
        ImmutableMultiset.copyOf(provider.namespaces()));

    provider.create("test2", "d1", testDescriptor);
    provider.create("test2", "d2", anotherDescriptor);
    Assert.assertEquals(ImmutableMultiset.of("test1", "test2"),
        ImmutableMultiset.copyOf(provider.namespaces()));

    provider.create("test3", "d1", testDescriptor);
    provider.create("test3", "d2", anotherDescriptor);
    Assert.assertEquals(ImmutableMultiset.of("test1", "test2", "test3"),
        ImmutableMultiset.copyOf(provider.namespaces()));

    provider.delete("test2", "d2");
    Assert.assertEquals(ImmutableMultiset.of("test1", "test2", "test3"),
        ImmutableMultiset.copyOf(provider.namespaces()));

    provider.delete("test2", "d1");
    Assert.assertEquals(ImmutableMultiset.of("test1", "test3"),
        ImmutableMultiset.copyOf(provider.namespaces()));

    provider.delete("test3", "d2");
    Assert.assertEquals(ImmutableMultiset.of("test1", "test3"),
        ImmutableMultiset.copyOf(provider.namespaces()));

    provider.delete("test3", "d1");
    Assert.assertEquals(ImmutableMultiset.of("test1"),
        ImmutableMultiset.copyOf(provider.namespaces()));

    provider.delete("test1", "d1");
    Assert.assertEquals(ImmutableMultiset.of("test1"),
        ImmutableMultiset.copyOf(provider.namespaces()));

    provider.delete("test1", "d2");
    Assert.assertEquals(ImmutableMultiset.<String>of(),
        ImmutableMultiset.copyOf(provider.namespaces()));
  }

  @Test
  public void testCustomProperties() {
    final String propName = "my.custom.property";
    final String propValue = "string";
    DatasetDescriptor descriptorWithProp =
        new DatasetDescriptor.Builder(testDescriptor)
        .property(propName, propValue)
        .build();

    DatasetDescriptor created = provider.create(NAMESPACE, NAME, descriptorWithProp);
    Assert.assertTrue("Should have custom property",
        created.hasProperty(propName));
    Assert.assertEquals("Should have correct custom property value",
        propValue, created.getProperty(propName));
    Assert.assertTrue("List should contain property name",
        created.listProperties().contains(propName));

    DatasetDescriptor loaded = provider.load(NAMESPACE, NAME);
    Assert.assertTrue("Should have custom property",
        loaded.hasProperty(propName));
    Assert.assertEquals("Should have correct custom property value",
        propValue, loaded.getProperty(propName));
    Assert.assertTrue("List should contain property name",
        created.listProperties().contains(propName));
  }
}
