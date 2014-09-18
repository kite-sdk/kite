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

import java.io.IOException;
import java.net.URI;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Formats;
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.ValidationException;

/**
 * Tests external URI backward-compatibility with existing datasets.
 *
 * Before namespaces, all tables were stored in the metastore using the default
 * namespace. External tables now use the containing folder name as the
 * namespace, so the old metadata will not be found because the namespace no
 * longer matches. The fix is to check the default namespace for the table when
 * the table isn't found.
 *
 * This problem doesn't affect other FileSystemDatasets because the metadata
 * for non-Hive datasets is stored along with the data. The change to using
 * namespaces was a change in internal book-keeping.
 *
 * This problem doesn't affect managed datasets because the managed URI doesn't
 * require a namespace. The namespace will be defaulted to "default" if no
 * namespace is included, which matches the old URI syntax. Failure to find the
 * dataset would only happen if the dataset was previously created with a
 * useless namespace query option.
 */
public class TestExternalBackwardCompatibility {

  private Configuration conf;
  private DatasetDescriptor descriptor = null;
  private MetaStoreUtil metastore = null;

  @Before
  public void addTableToDefault() {
    // this test uses the local FS because
    this.conf = new Configuration();
    this.metastore = new MetaStoreUtil(conf);
    cleanHive();
    metastore.dropTable("default", "test");
    this.descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral("\"string\"")
        .location(URI.create("file:/tmp/datasets/test"))
        .build();
    Table table = HiveUtils.tableForDescriptor(
        "default", "test", descriptor, true);
    metastore.createTable(table);
  }

  @After
  public void cleanHive() {
    // ensures all tables are removed
    MetaStoreUtil metastore = new MetaStoreUtil(conf);
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
  public void testLoadChecksDefaultNamespace() {
    Assert.assertNotNull("Should find dataset by checking default db",
        Datasets.load("dataset:hive:/tmp/datasets/test"));
    TestHelpers.assertThrows("Should not load dataset (there isn't one)",
        DatasetNotFoundException.class, new Runnable() {
          @Override
          public void run() {
            Datasets.load("dataset:hive:/tmp/datasets/test2");
          }
        });
  }

  @Test
  public void testLoadWithUpdatedURI() {
    // Adding the namespace in the URI works because the data location is kept
    Dataset<GenericRecord> ds = Datasets.load(
        "dataset:hive:/tmp/datasets/default/test");
    Assert.assertNotNull("Should find dataset with new URI", ds);
    Assert.assertEquals("Storage location should be unchanged",
        URI.create("file:/tmp/datasets/test"),
        ds.getDescriptor().getLocation());
  }

  @Test
  public void testExistsChecksDefaultNamespace() {
    Assert.assertTrue("Should find dataset by checking default db",
        Datasets.exists("dataset:hive:/tmp/datasets/test"));
    Assert.assertFalse("Should not find dataset (there isn't one)",
        Datasets.exists("dataset:hive:/tmp/datasets/test2"));
  }

  @Test
  public void testExistsWithUpdatedURI() {
    Assert.assertTrue("Should find dataset by checking default db",
        Datasets.exists("dataset:hive:/tmp/datasets/default/test"));
  }

  @Test
  public void testCreateSucceedsIfCompatible() {
    // this will create a Dataset in the right namespace/name that is backed
    // by the same data location. this can be used to migrate.
    Assert.assertNotNull("Create should succeed even if there is a duplicate",
        Datasets.create("dataset:hive:/tmp/datasets/test", descriptor));
  }

  @Test
  public void testCreateFailsIfNotCompatible() {
    // this will fail because the new descriptor uses a different format
    // the old descriptor is found and used to validate the change
    TestHelpers.assertThrows("Create should fail because of a format change",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            Datasets.create("dataset:hive:/tmp/datasets/test",
                new DatasetDescriptor.Builder(descriptor)
                    .format(Formats.PARQUET)
                    .build());
          }
        });
  }

  @Test
  public void testCreateIncompatibleSucceedsWithLocation() {
    // if there is a requested location then the default table isn't checked
    // because only the default location would have been used
    Assert.assertNotNull("Create should succeed if location doesn't match",
        Datasets.create("dataset:hive:/tmp/datasets/test",
            new DatasetDescriptor.Builder(descriptor)
            .location(URI.create("file:/tmp/test-data/test"))
            .format(Formats.PARQUET)
            .build()));
  }

  @Test
  public void testUpdateChangesDefaultNamespace() {
    Dataset<GenericRecord> updated = Datasets.update(
        "dataset:hive:/tmp/datasets/test",
        new DatasetDescriptor.Builder(descriptor)
            .property("added.property", "true")
            .build());
    Assert.assertNotNull("Update should succeed", updated);

    DatasetDescriptor stored =
        HiveUtils.descriptorForTable(conf, metastore.getTable("default", "test"));

    Assert.assertEquals("Should update default.test descriptor",
        stored, updated.getDescriptor());

    Assert.assertEquals("Added property should be present",
        stored.getProperty("added.property"), "true");
  }

  @Test
  public void testUpdateWithUpdatedURI() {
    Dataset<GenericRecord> updated = Datasets.update(
        "dataset:hive:/tmp/datasets/default/test",
        new DatasetDescriptor.Builder(descriptor)
            .property("added.property", "true")
            .build());
    Assert.assertNotNull("Update should succeed", updated);

    DatasetDescriptor stored =
        HiveUtils.descriptorForTable(conf, metastore.getTable("default", "test"));

    Assert.assertEquals("Should update default.test descriptor",
        stored, updated.getDescriptor());

    Assert.assertEquals("Added property should be present",
        stored.getProperty("added.property"), "true");
  }

  @Test
  public void testUpdateValidatesAgainstDefaultNamespace() {
    TestHelpers.assertThrows("Update should fail because of a format change",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            Datasets.update("dataset:hive:/tmp/datasets/test",
                new DatasetDescriptor.Builder(descriptor)
                    .format(Formats.PARQUET)
                    .build());
          }
        });
  }

  @Test
  public void testDeleteWithDefaultNamespace() throws IOException {
    Assert.assertTrue("Delete should succeed if the location matches",
        Datasets.delete("dataset:hive:/tmp/datasets/test"));
    Assert.assertFalse("Delete should return false if there is no dataset",
        Datasets.delete("dataset:hive:/tmp/datasets/test"));

    // recreate the default.test dataset, but with a different storage location
    DatasetDescriptor doNotDelete = new DatasetDescriptor.Builder(descriptor)
        .location(URI.create("file:/tmp/datasets/default/test"))
        .build();
    metastore.createTable(HiveUtils.tableForDescriptor(
        "default", "test", doNotDelete, true));

    Assert.assertFalse("Delete should not find a dataset to delete",
        Datasets.delete("dataset:hive:/tmp/datasets/test"));

    Assert.assertTrue("Delete should not change the dataset",
        metastore.exists("default", "test"));
  }

  @Test
  public void testDeleteWithUpdatedURI() {
    Assert.assertTrue("Delete should succeed with explicit namespace",
        Datasets.delete("dataset:hive:/tmp/datasets/default/test"));
    Assert.assertFalse("Delete should return false if there is no dataset",
        Datasets.delete("dataset:hive:/tmp/datasets/default/test"));
    Assert.assertFalse("Delete should remove the dataset",
        metastore.exists("default", "test"));
  }
}
