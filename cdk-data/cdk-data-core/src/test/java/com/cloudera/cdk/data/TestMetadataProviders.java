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

import static com.cloudera.cdk.data.filesystem.DatasetTestUtilities.*;
import com.google.common.collect.ImmutableMultiset;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public abstract class TestMetadataProviders {

  protected static final String NAME = "provider_test1";

  protected Configuration conf;
  protected DatasetDescriptor testDescriptor;
  protected DatasetDescriptor anotherDescriptor;

  protected MetadataProvider provider;
  abstract public MetadataProvider newProvider(Configuration conf);

  @Before
  public void setUp() throws IOException {
    this.conf = new Configuration();
    this.testDescriptor = new DatasetDescriptor.Builder()
        .format(Formats.AVRO)
        .schema(USER_SCHEMA_URL)
        .partitionStrategy(new PartitionStrategy.Builder()
            .year("timestamp")
            .month("timestamp")
            .day("timestamp")
            .get())
        .get();
    // something completely different
    this.anotherDescriptor = new DatasetDescriptor.Builder()
        .format(Formats.PARQUET)
        .schema(Schema.createArray(Schema.create(Schema.Type.FLOAT)))
        .partitionStrategy(new PartitionStrategy.Builder()
            .hash("some_field", 20000)
            .get())
        .get();

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
    // Location and Configuration are allowed to be set by the provider.
  }

  public void testCreateWithLocation() throws URISyntaxException {
    Assert.assertFalse("Sanity check", provider.exists(NAME));

    URI requestedLocation = new URI("hdfs:/tmp/data/my_data_set");
    DatasetDescriptor requested = new DatasetDescriptor.Builder(testDescriptor)
        .location(requestedLocation)
        .get();

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
    provider.create(NAME, new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA)
        .get());
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

  @Test(expected=NoSuchDatasetException.class)
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

    final DatasetDescriptor saved;
    try {
      saved = provider.update(NAME, anotherDescriptor);
    } catch (UnsupportedOperationException ex) {
      // this is okay, too
      return;
    }

    Assert.assertNotNull("Updated Descriptor should be returned", saved);
    Assert.assertEquals("Schema should match update",
        anotherDescriptor.getSchema(), saved.getSchema());
    Assert.assertEquals("PartitionStrategy should match update",
        anotherDescriptor.getPartitionStrategy(), saved.getPartitionStrategy());
    Assert.assertEquals("Format should match update",
        anotherDescriptor.getFormat(), saved.getFormat());
  }

  @Test(expected=NoSuchDatasetException.class)
  public void testUpdateFailsNoDataset() {
    try {
      provider.update(NAME, testDescriptor);
    } catch (UnsupportedOperationException ex) {
      // this is okay, too
      throw new NoSuchDatasetException(ex);
    }
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
}
