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

import static org.kitesdk.data.filesystem.DatasetTestUtilities.*;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class TestMetadataProviders extends MiniDFSTest {

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
        .schemaUri(USER_SCHEMA_URL)
        .partitionStrategy(new PartitionStrategy.Builder()
            .year("timestamp")
            .month("timestamp")
            .day("timestamp")
            .dateFormat("timestamp", "day", "yyyy-MM-dd")
            .build())
        .build();
    // something completely different
    this.anotherDescriptor = new DatasetDescriptor.Builder()
        .format(Formats.PARQUET)
        .schema(Schema.createArray(Schema.create(Schema.Type.FLOAT)))
        .partitionStrategy(new PartitionStrategy.Builder()
            .hash("some_field", 20000)
            .build())
        .build();

    this.provider = newProvider(conf);
  }

  @Test
  public void testCreate() {
    Assert.assertFalse("Sanity check", provider.exists(NAME));

    DatasetDescriptor created = provider.create(NAME, testDescriptor);

    Assert.assertNotNull("Descriptor should be returned", created);
    Assert.assertTrue("Descriptor should exist", provider.exists(NAME));
    Assert.assertEquals("Schema should match",
        testDescriptor.getSchema(), created.getSchema());
    Assert.assertEquals("PartitionStrategy should match",
        testDescriptor.getPartitionStrategy(), created.getPartitionStrategy());
    Assert.assertEquals("Format should match",
        testDescriptor.getFormat(), created.getFormat());
    Assert.assertNotNull("Location URI should be set",
        created.getLocation());
    Assert.assertNotNull("Location URI should have a scheme",
        created.getLocation().getScheme());
  }

  public void testCreateWithLocation() throws URISyntaxException {
    Assert.assertFalse("Sanity check", provider.exists(NAME));

    URI requestedLocation = new URI("hdfs:/tmp/data/my_data_set");
    DatasetDescriptor requested = new DatasetDescriptor.Builder(testDescriptor)
        .location(requestedLocation)
        .build();

    final DatasetDescriptor created;
    try {
      created = provider.create(NAME, requested);
    } catch (MetadataProviderException ex) {
      // this is expected if the provider doesn't support requested locations
      return;
    }

    // if supported, the location should be unchanged.
    Assert.assertNotNull("Descriptor should be returned", created);
    Assert.assertTrue("Descriptor should exist", provider.exists(NAME));
    Assert.assertEquals("Requested locations should match",
        requestedLocation, created.getLocation());
  }

  public void ensureCreated() {
    // use testCreate to create NAME
    testCreate();
    Assert.assertTrue("Sanity check", provider.exists(NAME));
  }

  @Test(expected=DatasetExistsException.class)
  public void testCreateAlreadyExists() {
    ensureCreated();
    provider.create(NAME, anotherDescriptor);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testCreateFailsNullName() {
    provider.create(null, testDescriptor);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testCreateFailsNullDescriptor() {
    provider.create(NAME, null);
  }

  @Test
  public void testLoad() {
    ensureCreated();

    DatasetDescriptor loaded = provider.load(NAME);

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
    Assert.assertFalse("Sanity check", provider.exists(NAME));
    provider.load(NAME);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testLoadFailsNullName() {
    provider.load(null);
  }

  public void testUpdate() {
    ensureCreated();

    /*
     * To be clear: we are testing that even crazy, incompatible changes are
     * happily saved by the MetadataProvider. Rule enforcement is done upstream
     * by libraries that are in a better position to make decisions about what
     * changes are incompatible.
     */

    final DatasetDescriptor saved = provider.update(NAME, anotherDescriptor);

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
    provider.update(NAME, testDescriptor);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testUpdateFailsNullName() {
    provider.update(null, testDescriptor);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testUpdateFailsNullDescriptor() {
    provider.update(NAME, null);
  }

  public void testDelete() {
    ensureCreated();

    boolean result = provider.delete(NAME);
    Assert.assertTrue("Delete descriptor should return true", result);

    result = provider.delete(NAME);
    Assert.assertFalse("Delete non-existent descriptor should return false", result);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testDeleteFailsNullName() {
    provider.delete(null);
  }

  @Test
  public void testExists() {
    Assert.assertFalse(provider.exists(NAME));

    provider.create(NAME, testDescriptor);
    Assert.assertTrue(provider.exists(NAME));

    provider.delete(NAME);
    Assert.assertFalse(provider.exists(NAME));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testExistsNullName() {
    provider.exists(null);
  }

  @Test
  public void testList() {
    Assert.assertEquals(ImmutableMultiset.of(),
        ImmutableMultiset.copyOf(provider.list()));

    provider.create("test1", testDescriptor);
    Assert.assertEquals(ImmutableMultiset.of("test1"),
        ImmutableMultiset.copyOf(provider.list()));

    provider.create("test2", testDescriptor);
    Assert.assertEquals(ImmutableMultiset.of("test1", "test2"),
        ImmutableMultiset.copyOf(provider.list()));

    provider.create("test3", testDescriptor);
    Assert.assertEquals(ImmutableMultiset.of("test1", "test2", "test3"),
        ImmutableMultiset.copyOf(provider.list()));

    provider.delete("test2");
    Assert.assertEquals(ImmutableMultiset.of("test1", "test3"),
        ImmutableMultiset.copyOf(provider.list()));

    provider.delete("test3");
    Assert.assertEquals(ImmutableMultiset.of("test1"),
        ImmutableMultiset.copyOf(provider.list()));

    provider.delete("test1");
    Assert.assertEquals(ImmutableMultiset.of(),
        ImmutableMultiset.copyOf(provider.list()));
  }

  @Test
  public void testCustomProperties() {
    final String propName = "my.custom.property";
    final String propValue = "string";
    DatasetDescriptor descriptorWithProp =
        new DatasetDescriptor.Builder(testDescriptor)
        .property(propName, propValue)
        .build();

    DatasetDescriptor created = provider.create(NAME, descriptorWithProp);
    junit.framework.Assert.assertTrue("Should have custom property",
        created.hasProperty(propName));
    junit.framework.Assert.assertEquals(
        "Should have correct custom property value",
        propValue, created.getProperty(propName));
    junit.framework.Assert.assertEquals("Should correctly list property names",
        Sets.newHashSet(propName), created.listProperties());

    DatasetDescriptor loaded = provider.load(NAME);
    junit.framework.Assert.assertTrue("Should have custom property",
        loaded.hasProperty(propName));
    junit.framework.Assert.assertEquals(
        "Should have correct custom property value",
        propValue, loaded.getProperty(propName));
    junit.framework.Assert.assertEquals("Should correctly list property names",
        Sets.newHashSet(propName), loaded.listProperties());
  }
}
