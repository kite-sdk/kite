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
package org.kitesdk.data.hbase.avro;

import org.kitesdk.data.DatasetException;
import org.kitesdk.data.spi.PartitionKey;
import org.kitesdk.data.hbase.avro.entities.CompositeRecord;
import org.kitesdk.data.hbase.avro.entities.SubRecord1;
import org.kitesdk.data.hbase.avro.entities.SubRecord2;
import org.kitesdk.data.hbase.impl.Dao;
import org.kitesdk.data.hbase.testing.HBaseTestUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CompositeDaoTest {

  private static final String subRecord1String;
  private static final String subRecord2String;
  private static final String tableName = "test_table";
  private HTablePool tablePool;

  static {
    try {
      subRecord1String = AvroUtils.inputStreamToString(AvroDaoTest.class
          .getResourceAsStream("/SubRecord1.avsc"));
      subRecord2String = AvroUtils.inputStreamToString(AvroDaoTest.class
          .getResourceAsStream("/SubRecord2.avsc"));
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
    // Construct Dao
    Dao<CompositeRecord> dao = SpecificAvroDao.buildCompositeDao(tablePool,
        tableName, Arrays.asList(subRecord1String, subRecord2String),
        CompositeRecord.class);

    // Construct records
    SubRecord1 subRecord1 = SubRecord1.newBuilder().setKeyPart1("1")
        .setKeyPart2("1").setField1("field1_1").setField2("field1_2").build();
    SubRecord2 subRecord2 = SubRecord2.newBuilder().setKeyPart1("1")
        .setKeyPart2("1").setField1("field2_1").setField2("field2_2").build();

    CompositeRecord compositeRecord = CompositeRecord.newBuilder()
        .setSubRecord1(subRecord1).setSubRecord2(subRecord2).build();

    // Test put and get
    dao.put(compositeRecord);

    PartitionKey key = new PartitionKey("1", "1");
    CompositeRecord returnedCompositeRecord = dao.get(key);
    assertEquals("field1_1", returnedCompositeRecord.getSubRecord1()
        .getField1());
    assertEquals("field1_2", returnedCompositeRecord.getSubRecord1()
        .getField2());
    assertEquals("field2_1", returnedCompositeRecord.getSubRecord2()
        .getField1());
    assertEquals("field2_2", returnedCompositeRecord.getSubRecord2()
        .getField2());

    // Test OCC
    assertFalse(dao.put(compositeRecord));
    assertTrue(dao.put(returnedCompositeRecord));

    // Test null field
    subRecord1 = SubRecord1.newBuilder().setKeyPart1("1").setKeyPart2("2")
        .setField1("field1_1").setField2("field1_2").build();
    compositeRecord = CompositeRecord.newBuilder().setSubRecord1(subRecord1)
        .build();
    dao.put(compositeRecord);
    key = new PartitionKey("1", "2");
    compositeRecord = dao.get(key);
    assertEquals(null, compositeRecord.getSubRecord2());
  }

  @Test
  public void testSpecificMap() throws Exception {
    // Construct Dao
    Dao<Map<String, SpecificRecord>> dao = SpecificAvroDao.buildCompositeDao(
        tablePool, tableName, 
        Arrays.asList(subRecord1String, subRecord2String));

    // Construct records and keys
    SubRecord1 subRecord1 = SubRecord1.newBuilder().setKeyPart1("1").setKeyPart2("1").setField1("field1_1")
        .setField2("field1_2").build();
    SubRecord2 subRecord2 = SubRecord2.newBuilder().setKeyPart1("1").setKeyPart2("1").setField1("field2_1")
        .setField2("field2_2").build();

    Map<String, SpecificRecord> compositeRecord = new HashMap<String, SpecificRecord>();
    compositeRecord.put("SubRecord1", subRecord1);
    compositeRecord.put("SubRecord2", subRecord2);

    // Test put and get
    dao.put(compositeRecord);
    PartitionKey key = new PartitionKey("1", "1");
    Map<String, SpecificRecord> returnedCompositeRecord = dao.get(key);
    assertEquals("field1_1", ((SubRecord1) returnedCompositeRecord.get("SubRecord1"))
        .getField1());
    assertEquals("field1_2", ((SubRecord1) returnedCompositeRecord.get("SubRecord1"))
        .getField2());
    assertEquals("field2_1", ((SubRecord2) returnedCompositeRecord.get("SubRecord2"))
        .getField1());
    assertEquals("field2_2", ((SubRecord2) returnedCompositeRecord.get("SubRecord2"))
        .getField2());

    // Test OCC
    assertFalse(dao.put(compositeRecord));
    assertTrue(dao.put(returnedCompositeRecord));

    // Test null field
    compositeRecord = new HashMap<String, SpecificRecord>();
    subRecord1 = SubRecord1.newBuilder().setKeyPart1("1").setKeyPart2("2")
        .setField1("field1_1").setField2("field1_2").build();
    compositeRecord.put("SubRecord1", subRecord1);
    dao.put(compositeRecord);
    key = new PartitionKey("1", "2");
    compositeRecord = dao.get(key);
    assertEquals(null, compositeRecord.get("SubRecord2"));
  }
  
  @Test(expected = DatasetException.class)
  public void testIncompatibleKeys() throws Exception {
    // Construct Dao
    Dao<Map<String, SpecificRecord>> dao = SpecificAvroDao.buildCompositeDao(
        tablePool, tableName, 
        Arrays.asList(subRecord1String, subRecord2String));
    
    // Construct records and keys
    SubRecord1 subRecord1 = SubRecord1.newBuilder().setKeyPart1("1").setKeyPart2("1").setField1("field1_1")
        .setField2("field1_2").build();
    SubRecord2 subRecord2 = SubRecord2.newBuilder().setKeyPart1("1").setKeyPart2("unmatched").setField1("field2_1")
        .setField2("field2_2").build();

    Map<String, SpecificRecord> compositeRecord = new HashMap<String, SpecificRecord>();
    compositeRecord.put("SubRecord1", subRecord1);
    compositeRecord.put("SubRecord2", subRecord2);

    // Test put and get
    dao.put(compositeRecord);
  }
}
