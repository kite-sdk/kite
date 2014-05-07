/**
 * Copyright 2014 Cloudera Inc.
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
package org.kitesdk.data.hbase;

import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.data.ColumnMapping;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.FieldMapping;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.hbase.avro.AvroUtils;
import org.kitesdk.data.hbase.impl.SchemaManager;
import org.kitesdk.data.hbase.manager.DefaultSchemaManager;
import org.kitesdk.data.hbase.testing.HBaseTestUtils;

import static org.junit.Assert.assertEquals;

public class HBaseMetadataProviderTest {

  private static final String testEntity;
  private static final String tableName = "testtable";
  private static final String managedTableName = "managed_schemas";
  private static HBaseMetadataProvider provider;

  static {
    try {
      testEntity = AvroUtils
          .inputStreamToString(HBaseMetadataProviderTest.class
              .getResourceAsStream("/TestEntity.avsc"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    Configuration conf = HBaseTestUtils.getMiniCluster().getConf();
    // managed table should be created by HBaseDatasetRepository
    HBaseTestUtils.util.deleteTable(Bytes.toBytes(managedTableName));

    HTablePool tablePool = new HTablePool(conf, 10);
    SchemaManager schemaManager = new DefaultSchemaManager(tablePool);
    HBaseAdmin admin = new HBaseAdmin(conf);
    provider = new HBaseMetadataProvider(admin, schemaManager);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HBaseTestUtils.util.deleteTable(Bytes.toBytes(tableName));
  }

  @After
  public void after() throws Exception {
    HBaseTestUtils.util.truncateTable(Bytes.toBytes(tableName));
    HBaseTestUtils.util.truncateTable(Bytes.toBytes(managedTableName));
  }

  @Test
  public void testBasic() {
    DatasetDescriptor desc = provider.create(tableName + ".TestEntity",
        new DatasetDescriptor.Builder().schemaLiteral(testEntity).build());
    ColumnMapping columnMapping = desc.getColumnMapping();
    PartitionStrategy partStrat = desc.getPartitionStrategy();
    assertEquals(9, columnMapping.getFieldMappings().size());
    assertEquals(2, partStrat.getFieldPartitioners().size());
  }

  @Test
  public void testOverride() {
    InputStream partStratIn = HBaseMetadataProviderTest.class
        .getResourceAsStream("/partition_strategy/AlternateTestEntity.json");
    InputStream columnMappingIn = HBaseMetadataProviderTest.class
        .getResourceAsStream("/column_mapping/AlternateTestEntity.json");

    DatasetDescriptor desc = provider.create(
        tableName + ".AlternateTestEntity", new DatasetDescriptor.Builder()
            .schemaLiteral(testEntity).partitionStrategy(partStratIn)
            .columnMapping(columnMappingIn).build());

    ColumnMapping columnMapping = desc.getColumnMapping();
    PartitionStrategy partStrat = desc.getPartitionStrategy();

    assertEquals(1, partStrat.getFieldPartitioners().size());
    assertEquals(8, columnMapping.getFieldMappings().size());

    assertEquals("alt", columnMapping.getFieldMapping("field1")
        .getFamilyAsString());
    assertEquals("field1", columnMapping.getFieldMapping("field1")
        .getQualifierAsString());
    assertEquals("altstring", columnMapping.getFieldMapping("field3")
        .getFamilyAsString());
    assertEquals(FieldMapping.MappingType.COLUMN, columnMapping.getFieldMapping("field4")
        .getMappingType());
  }
}
