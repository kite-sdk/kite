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

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.spi.DatasetRepositories;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.DefaultConfiguration;

public class TestHivePartitioning {

  @Before
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

  private static final Schema schema = SchemaBuilder.record("Record").fields()
      .requiredLong("timestamp")
      .requiredInt("group")
      .requiredString("id")
      .endRecord();

  private static final PartitionStrategy strategy = new PartitionStrategy.Builder()
      .year("timestamp")
      .month("timestamp")
      .day("timestamp")
      .hour("timestamp")
      .hash("id", 64)
      .identity("group")
      .identity("id")
      .identity("timestamp", "time_copy")
      .range("group", 5, 10, 15, 20)
      .range("id", "0", "a")
      .dateFormat("timestamp", "date", "yyyy-MM-dd")
      .build();

  private static final DatasetDescriptor descriptor = new DatasetDescriptor
      .Builder()
      .schema(SchemaBuilder.record("Record").fields()
          .requiredString("message")
          .requiredBoolean("bool")
          .requiredLong("timestamp")
          .requiredInt("number")
          .requiredDouble("double")
          .requiredFloat("float")
          .requiredBytes("payload")
          .endRecord())
      .partitionStrategy(new PartitionStrategy.Builder()
          .year("timestamp")
          .month("timestamp")
          .day("timestamp")
          .hour("timestamp")
          .minute("timestamp")
          .identity("message")
          .identity("timestamp", "ts")
          .identity("number", "num")
          .hash("message", 48)
          .hash("timestamp", 48)
          .hash("number", 48)
          .hash("payload", 48)
          .hash("float", 48)
          .hash("double", 48)
          .hash("bool", 48)
          .range("number", 5, 10, 15, 20)
          .range("message", "m", "z", "M", "Z")
          .dateFormat("timestamp", "date", "yyyy-MM-dd")
          .build())
      .build();

  @Test
  public void testNames() {
    List<FieldSchema> columns = HiveUtils.partitionColumns(strategy, schema);
    Assert.assertEquals("year", columns.get(0).getName());
    Assert.assertEquals("month", columns.get(1).getName());
    Assert.assertEquals("day", columns.get(2).getName());
    Assert.assertEquals("hour", columns.get(3).getName());
    Assert.assertEquals("id_hash", columns.get(4).getName());
    Assert.assertEquals("group_copy", columns.get(5).getName());
    Assert.assertEquals("id_copy", columns.get(6).getName());
    Assert.assertEquals("time_copy", columns.get(7).getName());
    Assert.assertEquals("group_bound", columns.get(8).getName());
    Assert.assertEquals("id_bound", columns.get(9).getName());
    Assert.assertEquals("date", columns.get(10).getName());
  }

  @Test
  public void testTypeMapping() {
    List<FieldSchema> columns = HiveUtils.partitionColumns(strategy, schema);
    Assert.assertEquals("int", columns.get(0).getType());
    Assert.assertEquals("int", columns.get(1).getType());
    Assert.assertEquals("int", columns.get(2).getType());
    Assert.assertEquals("int", columns.get(3).getType());
    Assert.assertEquals("int", columns.get(4).getType());
    Assert.assertEquals("int", columns.get(5).getType());
    Assert.assertEquals("string", columns.get(6).getType());
    Assert.assertEquals("bigint", columns.get(7).getType());
    Assert.assertEquals("int", columns.get(8).getType());
    Assert.assertEquals("string", columns.get(9).getType());
    Assert.assertEquals("string", columns.get(10).getType());
  }

  @Test
  public void testManagedDatasetCreation() {
    DatasetRepository repo = DatasetRepositories.repositoryFor("repo:hive");
    repo.delete("ns", "records"); // ensure it does not already exist
    repo.create("ns", "records", descriptor);
    repo.delete("ns", "records"); // clean up
  }

  @Test
  public void testExternalDatasetCreation() {
    DatasetRepository repo = DatasetRepositories.repositoryFor("repo:hive:target/");
    repo.delete("ns", "records"); // ensure it does not already exist
    repo.create("ns", "records", descriptor);
    repo.delete("ns", "records"); // clean up
  }

  @Test
  public void testProvidedPartitions() {
    PartitionStrategy expected = new PartitionStrategy.Builder()
        .provided("year", "int")
        .provided("month", "int")
        .provided("day", "long")
        .provided("us_state")
        .build();
    List<FieldSchema> partCols = Lists.newArrayList(
        new FieldSchema("year", "tinyint", "comment"),
        new FieldSchema("month", "int", "comment"),
        new FieldSchema("day", "bigint", "comment"),
        new FieldSchema("us_state", "string", "comment")
    );
    Assert.assertEquals("Should convert to correct provided types",
        expected, HiveUtils.fromPartitionColumns(partCols));
  }
}
