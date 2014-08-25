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
    Datasets.delete("dataset:hive:target/test-repo/external");
    // create datasets
    this.managed = DatasetRepositories.repositoryFor("repo:hive");
    managed.create("managed", descriptor);
    this.external = DatasetRepositories.repositoryFor("repo:hive:target/test-repo");
    external.create("external", descriptor);
  }

  @Test
  public void testManagedWithExternal() {
    HiveAbstractMetadataProvider provider = new HiveManagedMetadataProvider(
        new HiveConf());
    Assert.assertTrue(provider.isExternal("external"));

    Dataset<GenericData.Record> dataset = managed.load("external");
    Assert.assertNotNull("Should open external dataset with managed", dataset);
    Assert.assertEquals("Should match external dataset",
        external.load("external").getDescriptor(), dataset.getDescriptor());

    DatasetDescriptor updatedDescriptor =
        new DatasetDescriptor.Builder(dataset.getDescriptor())
            .property("kite.writer.cache-size", "34")
            .schemaLiteral("\"string\"")
            .build();

    Dataset<GenericData.Record> updated = managed
        .update("external", updatedDescriptor);
    Assert.assertNotNull("Should update external dataset with managed",
        updated);
    Assert.assertEquals("Should see changes in external dataset",
        external.load("external").getDescriptor(), updated.getDescriptor());

    Assert.assertTrue("Should delete external tables with managed",
        managed.delete("external"));
    TestHelpers.assertThrows("Should delete external table correctly",
        DatasetNotFoundException.class, new Runnable() {
          @Override
          public void run() {
            external.load("external");
          }
        });
  }

  @Test
  public void testExternalWithManaged() {
    HiveAbstractMetadataProvider provider = new HiveManagedMetadataProvider(
        new HiveConf());
    Assert.assertTrue(provider.isManaged("managed"));

    Dataset<GenericData.Record> dataset = external.load("managed");
    Assert.assertNotNull("Should open managed dataset with external", dataset);
    Assert.assertEquals("Should match managed dataset",
        managed.load("managed").getDescriptor(), dataset.getDescriptor());

    DatasetDescriptor updatedDescriptor =
        new DatasetDescriptor.Builder(dataset.getDescriptor())
            .property("kite.writer.cache-size", "34")
            .schemaLiteral("\"string\"")
            .build();

    Dataset<GenericData.Record> updated = external
        .update("managed", updatedDescriptor);
    Assert.assertNotNull("Should update managed dataset with external",
        updated);
    Assert.assertEquals("Should see changes in managed dataset",
        managed.load("managed").getDescriptor(), updated.getDescriptor());

    Assert.assertTrue("Should delete managed tables with external",
        external.delete("managed"));
    TestHelpers.assertThrows("Should delete managed table correctly",
        DatasetNotFoundException.class, new Runnable() {
          @Override
          public void run() {
            managed.load("managed");
          }
        });
  }

  @Test
  public void testRepositoryList() throws Exception {
    // create unreadable hive tables
    MetaStoreUtil metastore = new MetaStoreUtil(new Configuration());
    metastore.dropTable("default", "bad_type");
    metastore.dropTable("default", "bad_serde");
    metastore.dropTable("default", "bad_schema");

    Table badType = HiveUtils.createEmptyTable("bad_type");
    badType.setTableType(TableType.VIRTUAL_VIEW.toString());
    metastore.createTable(badType);

    Table badSerDe = HiveUtils.createEmptyTable("bad_serde");
    badSerDe.setTableType(TableType.MANAGED_TABLE.toString()); // readable type
    badSerDe.getSd().getSerdeInfo().setSerializationLib("com.example.ExampleHiveSerDe");
    metastore.createTable(badSerDe);

    Table badSchema = HiveUtils.createEmptyTable("bad_schema");
    badSchema.setTableType(TableType.MANAGED_TABLE.toString()); // readable type
    badSchema.getSd().getSerdeInfo().setSerializationLib("org.apache.hadoop.hive.serde2.avro.AvroSerDe");
    badSchema.getSd().setInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat");
    badSchema.getSd().setOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat");
    metastore.createTable(badSchema);

    // note that unreadable tables are not in the list
    Set<String> expected = Sets.newHashSet("managed", "external");
    Assert.assertEquals("Managed should list external and managed tables",
        expected, Sets.newHashSet(managed.list()));
    Assert.assertEquals("External should list external and managed tables",
        expected, Sets.newHashSet(external.list()));
  }
}
