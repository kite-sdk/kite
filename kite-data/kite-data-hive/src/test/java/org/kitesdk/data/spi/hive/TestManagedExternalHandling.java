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

import com.google.common.collect.Sets;
import java.util.Set;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.spi.DatasetRepositories;
import org.kitesdk.data.spi.DatasetRepository;

public class TestManagedExternalHandling {
  private static final DatasetDescriptor descriptor =
      new DatasetDescriptor.Builder()
          .schemaLiteral("\"string\"")
          .build();

  private DatasetRepository managed;
  private DatasetRepository external;

  @Before
  public void setupRepositories() {
    // ensure the datasets do not already exist
    Datasets.delete("dataset:hive?dataset=managed");
    Datasets.delete("dataset:hive:target/test-repo/ns/external");
    // ensure no other metadata is left in the metastore
    cleanHive();
    // create datasets
    this.managed = DatasetRepositories.repositoryFor("repo:hive");
    Datasets.create("dataset:hive?dataset=managed", descriptor);
    this.external = DatasetRepositories.repositoryFor("repo:hive:target/test-repo");
    external.create("ns", "external", descriptor);
  }

  @After
  public void cleanHive() {
    // ensures all tables are removed
    MetaStoreUtil metastore = new MetaStoreUtil(new Configuration());
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
  public void testManagedWithExternal() {
    HiveAbstractMetadataProvider provider = new HiveManagedMetadataProvider(
        new HiveConf());
    Assert.assertTrue(provider.isExternal("ns", "external"));

    Dataset<GenericData.Record> dataset = managed.load("ns", "external");
    Assert.assertNotNull("Should open external dataset with managed", dataset);
    Assert.assertEquals("Should match external dataset",
        external.load("ns", "external").getDescriptor(), dataset.getDescriptor());

    DatasetDescriptor updatedDescriptor =
        new DatasetDescriptor.Builder(dataset.getDescriptor())
            .property("kite.writer.cache-size", "34")
            .schemaLiteral("\"string\"")
            .build();

    Dataset<GenericData.Record> updated = managed
        .update("ns", "external", updatedDescriptor);
    Assert.assertNotNull("Should update external dataset with managed",
        updated);
    Assert.assertEquals("Should see changes in external dataset",
        external.load("ns", "external").getDescriptor(), updated.getDescriptor());

    Assert.assertTrue("Should delete external tables with managed",
        managed.delete("ns", "external"));
    TestHelpers.assertThrows("Should delete external table correctly",
        DatasetNotFoundException.class, new Runnable() {
          @Override
          public void run() {
            external.load("ns", "external");
          }
        });
  }

  @Test
  public void testExternalWithManaged() {
    HiveAbstractMetadataProvider provider = new HiveManagedMetadataProvider(
        new HiveConf());
    Assert.assertTrue(provider.isManaged("default", "managed"));

    Dataset<GenericData.Record> dataset = external.load("default", "managed");
    Assert.assertNotNull("Should open managed dataset with external", dataset);
    Assert.assertEquals("Should match managed dataset",
        managed.load("default", "managed").getDescriptor(), dataset.getDescriptor());

    DatasetDescriptor updatedDescriptor =
        new DatasetDescriptor.Builder(dataset.getDescriptor())
            .property("kite.writer.cache-size", "34")
            .schemaLiteral("\"string\"")
            .build();

    Dataset<GenericData.Record> updated = external
        .update("default", "managed", updatedDescriptor);
    Assert.assertNotNull("Should update managed dataset with external",
        updated);
    Assert.assertEquals("Should see changes in managed dataset",
        managed.load("default", "managed").getDescriptor(), updated.getDescriptor());

    Assert.assertTrue("Should delete managed tables with external",
        external.delete("default", "managed"));
    TestHelpers.assertThrows("Should delete managed table correctly",
        DatasetNotFoundException.class, new Runnable() {
          @Override
          public void run() {
            managed.load("default", "managed");
          }
        });
  }

  @Test
  public void testRepositoryList() throws Exception {
    // create unreadable hive tables
    MetaStoreUtil metastore = new MetaStoreUtil(new Configuration());
    metastore.dropTable("default", "bad_type");
    metastore.dropTable("bad", "bad_serde");
    metastore.dropTable("bad", "bad_schema");

    Table badType = HiveUtils.createEmptyTable("default", "bad_type");
    badType.setTableType(TableType.VIRTUAL_VIEW.toString());
    metastore.createTable(badType);

    Table badSerDe = HiveUtils.createEmptyTable("bad", "bad_serde");
    badSerDe.setTableType(TableType.MANAGED_TABLE.toString()); // readable type
    badSerDe.getSd().getSerdeInfo().setSerializationLib("com.example.ExampleHiveSerDe");
    metastore.createTable(badSerDe);

    Table badSchema = HiveUtils.createEmptyTable("bad", "bad_schema");
    badSchema.setTableType(TableType.MANAGED_TABLE.toString()); // readable type
    badSchema.getSd().getSerdeInfo().setSerializationLib("org.apache.hadoop.hive.serde2.avro.AvroSerDe");
    badSchema.getSd().setInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat");
    badSchema.getSd().setOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat");
    metastore.createTable(badSchema);

    // note that unreadable tables are not in the lists
    Set<String> expectedNamespaces = Sets.newHashSet("default", "ns");
    Assert.assertEquals("Managed should list namespaces with external and managed tables",
        expectedNamespaces, Sets.newHashSet(managed.namespaces()));
    Assert.assertEquals("External should list namespaces with external and managed tables",
        expectedNamespaces, Sets.newHashSet(external.namespaces()));

    Set<String> expectedInDefault = Sets.newHashSet("managed");
    Assert.assertEquals("Managed should list external and managed tables",
        expectedInDefault, Sets.newHashSet(managed.datasets("default")));
    Assert.assertEquals("External should list external and managed tables",
        expectedInDefault, Sets.newHashSet(external.datasets("default")));

    Set<String> expectedInNS = Sets.newHashSet("external");
    Assert.assertEquals("Managed should list external and managed tables",
        expectedInNS, Sets.newHashSet(managed.datasets("ns")));
    Assert.assertEquals("External should list external and managed tables",
        expectedInNS, Sets.newHashSet(external.datasets("ns")));
  }
}
