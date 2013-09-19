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
package com.cloudera.cdk.data.hbase.avro;

import com.cloudera.cdk.data.Dataset;
import com.cloudera.cdk.data.DatasetAccessor;
import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.PartitionKey;
import com.cloudera.cdk.data.PartitionStrategy;
import com.cloudera.cdk.data.hbase.HBaseDatasetRepository;
import com.cloudera.cdk.data.hbase.avro.entities.SubEntity1;
import com.cloudera.cdk.data.hbase.avro.entities.SubEntity2;
import com.cloudera.cdk.data.hbase.avro.impl.AvroUtils;
import com.cloudera.cdk.data.hbase.testing.HBaseTestUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class CompositeDatasetTest {

  private static final String subEntity1String;
  private static final String subEntity2String;
  private static final String tableName = "testtable";
  private HTablePool tablePool;

  static {
    try {
      subEntity1String = AvroUtils.inputStreamToString(AvroDaoTest.class
          .getResourceAsStream("/SubEntity1.avsc"));
      subEntity2String = AvroUtils.inputStreamToString(AvroDaoTest.class
          .getResourceAsStream("/SubEntity2.avsc"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    HBaseTestUtils.getMiniCluster();
    byte[] tableNameBytes = Bytes.toBytes(tableName);
    byte[][] cfNames = { Bytes.toBytes("meta"), Bytes.toBytes("conflict"),
        Bytes.toBytes("_s") };
    HBaseTestUtils.util.createTable(tableNameBytes, cfNames);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HBaseTestUtils.util.deleteTable(Bytes.toBytes(tableName));
  }

  @Before
  public void beforeTest() throws Exception {
    tablePool = new HTablePool(HBaseTestUtils.getConf(), 10);
  }

  @After
  public void afterTest() throws Exception {
    tablePool.close();
  }

  @Test
  public void testSpecific() throws Exception {

    HBaseAdmin hBaseAdmin = new HBaseAdmin(HBaseTestUtils.getConf());
    HBaseDatasetRepository repo = new HBaseDatasetRepository(hBaseAdmin, tablePool);

    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder()
        .identity("part1", 1).identity("part2", 2).get();

    Schema compositeSchema = Schema.createRecord("CompositeSchema", null, null, false);
    compositeSchema.setFields(Lists.newArrayList(
        new Schema.Field("SubEntity1", SubEntity1.SCHEMA$, null, null),
        new Schema.Field("SubEntity2", SubEntity2.SCHEMA$, null, null)
    ));

    // create constituent datasets
    repo.create(tableName, new DatasetDescriptor.Builder()
        .schema(SubEntity1.SCHEMA$)
        .partitionStrategy(partitionStrategy)
        .get());
    repo.create(tableName, new DatasetDescriptor.Builder()
        .schema(SubEntity2.SCHEMA$)
        .partitionStrategy(partitionStrategy)
        .get());

    // create composite dataset
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(compositeSchema)
        .partitionStrategy(partitionStrategy)
        .get();
    Dataset ds = repo.create(tableName, descriptor);
    DatasetAccessor<Map> accessor = ds.newAccessor();

    // Construct entities
    SubEntity1 subEntity1 = SubEntity1.newBuilder().setPart1("1").setPart2("1")
        .setField1("field1_1").setField2("field1_2").build();
    SubEntity2 subEntity2 = SubEntity2.newBuilder().setPart1("1").setPart2("1")
        .setField1("field2_1").setField2("field2_2").build();

    Map<String, SpecificRecord> compositeEntity = Maps.newHashMap();
    compositeEntity.put("SubEntity1", subEntity1);
    compositeEntity.put("SubEntity2", subEntity2);

    // Test put and get
    accessor.put(compositeEntity);

    PartitionKey key = partitionStrategy.partitionKey("1", "1");
    Map<String, SpecificRecord> returnedCompositeEntity = accessor.get(key);
    assertNotNull("found entity", returnedCompositeEntity);
    assertEquals("field1_1", ((SubEntity1) returnedCompositeEntity.get("SubEntity1"))
        .getField1());
    assertEquals("field1_2", ((SubEntity1) returnedCompositeEntity.get("SubEntity1"))
        .getField2());
    assertEquals("field2_1", ((SubEntity2) returnedCompositeEntity.get("SubEntity2"))
        .getField1());
    assertEquals("field2_2", ((SubEntity2) returnedCompositeEntity.get("SubEntity2"))
        .getField2());

    // Test OCC
    assertFalse(accessor.put(compositeEntity));
    assertTrue(accessor.put(returnedCompositeEntity));

    // Test null field
    subEntity1 = SubEntity1.newBuilder(subEntity1).setPart2("2").build(); // different key
    compositeEntity.clear();
    compositeEntity.put("SubEntity1", subEntity1);
    accessor.put(compositeEntity);
    returnedCompositeEntity = accessor.get(partitionStrategy.partitionKey("1", "2"));
    // unlike using the Dao directly, the returned entity is not null since it contains
    // the key, but its non-key fields are null
    assertNotNull(returnedCompositeEntity.get("SubEntity2"));
    assertNull(((SubEntity2) returnedCompositeEntity.get("SubEntity2")).getField1());
    assertNull(((SubEntity2) returnedCompositeEntity.get("SubEntity2")).getField2());
  }
}
