/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spi.hive;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.spi.DatasetRepositories;
import org.kitesdk.data.spi.DatasetRepository;

public class TestHiveDatasetURIsCompatibility {

  private static final DatasetDescriptor DESCRIPTOR = new DatasetDescriptor
      .Builder()
      .schemaLiteral("\"string\"")
      .build();

  private static final MetaStoreUtil metastore = new MetaStoreUtil(new Configuration());

  @Before
  @After
  public void cleanHive() {
    // ensures all tables are removed
    for (String database : metastore.getAllDatabases()) {
      for (String table : metastore.getAllTables(database)) {
        metastore.dropTable(database, table);
      }
      if (!"default".equals(database)) {
        metastore.dropDatabase(database, true);
      }
    }
  }

  @Test
  public void testRelativePathURI() {
    // recognized as a deprecated form because there are 3 path components
    String uri = "dataset:hive:data/ns/ds";

    Datasets.create(uri, DESCRIPTOR);
    Table table = metastore.getTable("ns", "ds");

    Assert.assertNotNull("Table should be found under ns.ds", table);
    Assert.assertTrue("Should create an external table",
        HiveAbstractMetadataProvider.isExternal(table));

    Assert.assertTrue(Datasets.delete(uri));
  }

  @Test
  public void testCreateChangedRelativePathURI() {
    // this used to be a relative external URI, but is now a managed URI
    String uri = "dataset:hive:ns/ds";

    Datasets.create(uri, DESCRIPTOR);
    Table table = metastore.getTable("ns", "ds");

    Assert.assertNotNull("Table should be found under ns.ds", table);
    Assert.assertTrue("Should create a managed table: " + table.getSd().getLocation(),
        HiveAbstractMetadataProvider.isManaged(table));

    Assert.assertTrue(Datasets.delete(uri));
  }

  @Test
  public void testCreateChangedRelativePathURIWithDescriptorLocation() {
    String uri = "dataset:hive:ns/ds";

    Datasets.create(uri, new DatasetDescriptor.Builder(DESCRIPTOR)
        .location("file:/tmp/data/ns/ds")
        .build());

    Table table = metastore.getTable("ns", "ds");

    Assert.assertNotNull("Table should be found under ns.ds", table);
    Assert.assertTrue("Should create an external table",
        HiveAbstractMetadataProvider.isExternal(table));

    Assert.assertTrue(Datasets.delete(uri));
  }

  @Test
  public void testCreateChangedRelativePathURIWithURILocation() {
    String uri = "dataset:hive:ns/ds?location=file:/tmp/data/ns/ds";

    Datasets.create(uri, DESCRIPTOR);

    Table table = metastore.getTable("ns", "ds");

    Assert.assertNotNull("Table should be found under ns.ds", table);
    Assert.assertTrue("Should create an external table",
        HiveAbstractMetadataProvider.isExternal(table));

    Assert.assertTrue(Datasets.delete(uri));
  }

  @Test
  public void testLoadChangedRelativePathURI() {
    // this used to be a relative external URI, but is now a managed URI
    String uri = "dataset:hive:ns/ds";

    DatasetRepository repo = DatasetRepositories
        .repositoryFor("repo:hive:/tmp/data");
    Dataset<GenericRecord> expected = repo.create(
        "ns", "ds", DESCRIPTOR, GenericRecord.class);

    Dataset<GenericRecord> actual = Datasets.load(uri);
    Assert.assertEquals("Should load existing dataset ns.ds",
        expected, actual);

    Assert.assertEquals("URI should use apparent namespace",
        "dataset:hive:ns/ds", actual.getUri().toString());

    Assert.assertTrue(Datasets.delete(uri));
  }

  @Test
  public void testLoadChangedRelativePathURICompatibility() {
    // this used to be a relative external URI, but is now a managed URI
    String uri = "dataset:hive:data/ds";

    DatasetRepository repo = DatasetRepositories
        .repositoryFor("repo:hive:/tmp/data");
    DatasetDescriptor withLocation = new DatasetDescriptor.Builder(DESCRIPTOR)
        .location("file:/tmp/data/ds") // old location
        .build();
    Dataset<GenericRecord> expected = repo.create(
        "default", "ds", withLocation, GenericRecord.class);

    Dataset<GenericRecord> actual = Datasets.load(uri);
    Assert.assertEquals("Should load existing dataset default.ds",
        expected, actual);

    Assert.assertEquals("URI should use apparent namespace",
        "dataset:hive:data/ds", actual.getUri().toString());

    Assert.assertTrue(Datasets.delete(uri));
  }


  @Test
  public void testCreateChangedRelativePathURIMissingNamespace() {
    // this used to be a relative external URI, but is now a managed URI
    String uri = "dataset:hive:ds";

    Datasets.create(uri, DESCRIPTOR);
    Table table = metastore.getTable("default", "ds");

    Assert.assertNotNull("Table should be found under default.ds", table);
    Assert.assertTrue("Should create a managed table: " + table.getSd().getLocation(),
        HiveAbstractMetadataProvider.isManaged(table));

    Assert.assertTrue(Datasets.delete(uri));
  }

  @Test
  public void testCreateChangedRelativePathURIMissingNamespaceWithDescriptorLocation() {
    String uri = "dataset:hive:ds";

    Datasets.create(uri, new DatasetDescriptor.Builder(DESCRIPTOR)
        .location("file:/tmp/data/ns/ds")
        .build());

    Table table = metastore.getTable("default", "ds");

    Assert.assertNotNull("Table should be found under default.ds", table);
    Assert.assertTrue("Should create an external table",
        HiveAbstractMetadataProvider.isExternal(table));

    Assert.assertTrue(Datasets.delete(uri));
  }

  @Test
  public void testCreateChangedRelativePathURIMissingNamespaceWithURILocation() {
    String uri = "dataset:hive:ds?location=file:/tmp/data/ns/ds";

    Datasets.create(uri, DESCRIPTOR);

    Table table = metastore.getTable("default", "ds");

    Assert.assertNotNull("Table should be found under default.ds", table);
    Assert.assertTrue("Should create an external table",
        HiveAbstractMetadataProvider.isExternal(table));

    Assert.assertTrue(Datasets.delete(uri));
  }

  @Test
  public void testLoadChangedRelativePathURIMissingNamespace() {
    // this used to be a relative external URI, but is now a managed URI
    String uri = "dataset:hive:ds";

    DatasetRepository repo = DatasetRepositories
        .repositoryFor("repo:hive:/tmp/data");
    DatasetDescriptor withLocation = new DatasetDescriptor.Builder(DESCRIPTOR)
        .location("file:/tmp/data/ds") // old location
        .build();
    Dataset<GenericRecord> expected = repo.create(
        "default", "ds", withLocation, GenericRecord.class);

    Dataset<GenericRecord> actual = Datasets.load(uri);
    Assert.assertEquals("Should load existing dataset default.ds",
        expected, actual);

    Assert.assertEquals("URI should use actual namespace",
        "dataset:hive:default/ds", actual.getUri().toString());

    Assert.assertTrue(Datasets.delete(uri));
  }

  @Test
  public void testAbsolutePathURI() {
    // recognized as a absolute because there are 3 path components
    String uri = "dataset:hive:/tmp/data/ns/ds";

    Datasets.create(uri, DESCRIPTOR);
    Table table = metastore.getTable("ns", "ds");

    Assert.assertNotNull("Table should be found under ns.ds", table);
    Assert.assertTrue("Should create an external table",
        HiveAbstractMetadataProvider.isExternal(table));

    Assert.assertTrue(Datasets.delete(uri));
  }

  @Test
  public void testCreateChangedAbsolutePathURI() {
    // this used to be a relative external URI, but is now a managed URI
    String uri = "dataset:hive:/ns/ds";

    Datasets.create(uri, DESCRIPTOR);
    Table table = metastore.getTable("ns", "ds");

    Assert.assertNotNull("Table should be found under ns.ds", table);
    Assert.assertTrue("Should create a managed table: " + table.getSd().getLocation(),
        HiveAbstractMetadataProvider.isManaged(table));

    Assert.assertTrue(Datasets.delete(uri));
  }

  @Test
  public void testCreateChangedAbsolutePathURIWithDescriptorLocation() {
    String uri = "dataset:hive:/ns/ds";

    Datasets.create(uri, new DatasetDescriptor.Builder(DESCRIPTOR)
        .location("file:/tmp/data/ns/ds")
        .build());

    Table table = metastore.getTable("ns", "ds");

    Assert.assertNotNull("Table should be found under ns.ds", table);
    Assert.assertTrue("Should create an external table",
        HiveAbstractMetadataProvider.isExternal(table));

    Assert.assertTrue(Datasets.delete(uri));
  }

  @Test
  public void testCreateChangedAbsolutePathURIWithURILocation() {
    String uri = "dataset:hive:/ns/ds?location=file:/tmp/data/ns/ds";

    Datasets.create(uri, DESCRIPTOR);

    Table table = metastore.getTable("ns", "ds");

    Assert.assertNotNull("Table should be found under ns.ds", table);
    Assert.assertTrue("Should create an external table",
        HiveAbstractMetadataProvider.isExternal(table));

    Assert.assertTrue(Datasets.delete(uri));
  }

  @Test
  public void testLoadChangedAbsolutePathURI() {
    // this used to be a relative external URI, but is now a managed URI
    String uri = "dataset:hive:/ns/ds";

    DatasetRepository repo = DatasetRepositories
        .repositoryFor("repo:hive:/tmp/data");
    Dataset<GenericRecord> expected = repo.create(
        "ns", "ds", DESCRIPTOR, GenericRecord.class);

    Dataset<GenericRecord> actual = Datasets.load(uri);
    Assert.assertEquals("Should load existing dataset ns.ds",
        expected, actual);

    Assert.assertEquals("URI should use apparent namespace",
        "dataset:hive:ns/ds", actual.getUri().toString());

    Assert.assertTrue(Datasets.delete(uri));
  }

  @Test
  public void testLoadChangedAbsolutePathURICompatibility() {
    // this used to be a relative external URI, but is now a managed URI
    String uri = "dataset:hive:/data/ds";

    DatasetRepository repo = DatasetRepositories
        .repositoryFor("repo:hive:/tmp/data");
    DatasetDescriptor withLocation = new DatasetDescriptor.Builder(DESCRIPTOR)
        .location("file:/tmp/data/ds") // old location
        .build();
    Dataset<GenericRecord> expected = repo.create(
        "default", "ds", withLocation, GenericRecord.class);

    Dataset<GenericRecord> actual = Datasets.load(uri);
    Assert.assertEquals("Should load existing dataset default.ds",
        expected, actual);

    Assert.assertEquals("URI should use apparent namespace",
        "dataset:hive:data/ds", actual.getUri().toString());

    Assert.assertTrue(Datasets.delete(uri));
  }


  @Test
  public void testCreateChangedAbsolutePathURIMissingNamespace() {
    // this used to be a relative external URI, but is now a managed URI
    String uri = "dataset:hive:/ds";

    Datasets.create(uri, DESCRIPTOR);
    Table table = metastore.getTable("default", "ds");

    Assert.assertNotNull("Table should be found under default.ds", table);
    Assert.assertTrue("Should create a managed table: " + table.getSd().getLocation(),
        HiveAbstractMetadataProvider.isManaged(table));

    Assert.assertTrue(Datasets.delete(uri));
  }

  @Test
  public void testCreateChangedAbsolutePathURIMissingNamespaceWithDescriptorLocation() {
    String uri = "dataset:hive:/ds";

    Datasets.create(uri, new DatasetDescriptor.Builder(DESCRIPTOR)
        .location("file:/tmp/data/ns/ds")
        .build());

    Table table = metastore.getTable("default", "ds");

    Assert.assertNotNull("Table should be found under default.ds", table);
    Assert.assertTrue("Should create an external table",
        HiveAbstractMetadataProvider.isExternal(table));

    Assert.assertTrue(Datasets.delete(uri));
  }

  @Test
  public void testCreateChangedAbsolutePathURIMissingNamespaceWithURILocation() {
    String uri = "dataset:hive:/ds?location=file:/tmp/data/ns/ds";

    Datasets.create(uri, DESCRIPTOR);

    Table table = metastore.getTable("default", "ds");

    Assert.assertNotNull("Table should be found under default.ds", table);
    Assert.assertTrue("Should create an external table",
        HiveAbstractMetadataProvider.isExternal(table));

    Assert.assertTrue(Datasets.delete(uri));
  }

  @Test
  public void testLoadChangedAbsolutePathURIMissingNamespace() {
    // this used to be a relative external URI, but is now a managed URI
    String uri = "dataset:hive:/ds";

    DatasetRepository repo = DatasetRepositories
        .repositoryFor("repo:hive:/tmp/data");
    DatasetDescriptor withLocation = new DatasetDescriptor.Builder(DESCRIPTOR)
        .location("file:/tmp/data/ds") // old location
        .build();
    Dataset<GenericRecord> expected = repo.create(
        "default", "ds", withLocation, GenericRecord.class);

    Dataset<GenericRecord> actual = Datasets.load(uri);
    Assert.assertEquals("Should load existing dataset default.ds",
        expected, actual);

    Assert.assertEquals("URI should use actual namespace",
        "dataset:hive:default/ds", actual.getUri().toString());

    Assert.assertTrue(Datasets.delete(uri));
  }

}
