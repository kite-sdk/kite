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
import org.kitesdk.data.hbase.avro.entities.ArrayRecord;
import org.kitesdk.data.hbase.avro.entities.EmbeddedRecord;
import org.kitesdk.data.hbase.avro.entities.TestEnum;
import org.kitesdk.data.hbase.avro.entities.TestIncrement;
import org.kitesdk.data.hbase.avro.entities.TestRecord;
import org.kitesdk.data.hbase.impl.Dao;
import org.kitesdk.data.hbase.impl.EntityBatch;
import org.kitesdk.data.hbase.impl.EntityScanner;
import org.kitesdk.data.hbase.testing.HBaseTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AvroDaoTest {

  private static final String schemaString;
  private static final String incrementSchemaString;
  private static final String tableName = "test_table";
  private static final String incrementTableName = "test_increment_table";
  private HTablePool tablePool;

  static {
    try {
      schemaString = AvroUtils.inputStreamToString(AvroDaoTest.class
          .getResourceAsStream("/TestRecord.avsc"));
      incrementSchemaString = AvroUtils.inputStreamToString(AvroDaoTest.class
          .getResourceAsStream("/TestIncrement.avsc"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    HBaseTestUtils.getMiniCluster();
    byte[] tableNameBytes = Bytes.toBytes(tableName);
    byte[] incrementTableNameBytes = Bytes.toBytes(incrementTableName);
    byte[][] cfNames = { Bytes.toBytes("meta"), Bytes.toBytes("string"),
        Bytes.toBytes("embedded"), Bytes.toBytes("_s") };
    HBaseTestUtils.util.createTable(tableNameBytes, cfNames);
    HBaseTestUtils.util.createTable(incrementTableNameBytes, cfNames);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HBaseTestUtils.util.deleteTable(Bytes.toBytes(tableName));
    HBaseTestUtils.util.deleteTable(Bytes.toBytes(incrementTableName));
  }

  @Before
  public void beforeTest() throws Exception {
    HBaseTestUtils.util.truncateTable(Bytes.toBytes(tableName));
    tablePool = new HTablePool(HBaseTestUtils.getConf(), 10);
  }

  @After
  public void afterTest() {
    try {
      tablePool.close();
    } catch (Exception e) {
    }
  }

  @Test
  public void testGeneric() throws Exception {
    Dao<GenericRecord> dao = new GenericAvroDao(tablePool, tableName,
        schemaString);

    for (int i = 0; i < 10; ++i) {
      @SuppressWarnings("deprecation")
      GenericRecord entity = new GenericData.Record(Schema.parse(schemaString));
      entity.put("keyPart1", "part1_" + i);
      entity.put("keyPart2", "part2_" + i);
      entity.put("field1", "field1_" + i);
      entity.put("field2", "field2_" + i);
      dao.put(entity);
    }

    for (int i = 0; i < 10; ++i) {
      PartitionKey key = new PartitionKey(
          "part1_" + Integer.toString(i), "part2_" + Integer.toString(i));
      GenericRecord genericRecord = dao.get(key);
      assertEquals("field1_" + i, genericRecord.get("field1").toString());
      assertEquals("field2_" + i, genericRecord.get("field2").toString());
    }

    int cnt = 0;
    EntityScanner<GenericRecord> entityScanner = dao.getScanner();
    entityScanner.initialize();
    try {
      for (GenericRecord entity : entityScanner) {
        assertEquals("field1_" + cnt, entity.get("field1").toString());
        assertEquals("field2_" + cnt, entity.get("field2").toString());
        cnt++;
      }
      assertEquals(10, cnt);
    } finally {
      entityScanner.close();
    }

    cnt = 5;
    PartitionKey startKey = new PartitionKey("part1_5");
    entityScanner = dao.getScanner(startKey, null);
    entityScanner.initialize();
    try {
      for (GenericRecord entity : entityScanner) {
        assertEquals("field1_" + cnt, entity.get("field1").toString());
        assertEquals("field2_" + cnt, entity.get("field2").toString());
        cnt++;
      }
      assertEquals(10, cnt);
    } finally {
      if (entityScanner != null) {
        entityScanner.close();
      }
    }

    PartitionKey key = new PartitionKey("part1_5", "part2_5");
    dao.delete(key);
    GenericRecord deletedRecord = dao.get(key);
    assertNull(deletedRecord);
  }

  @Test
  public void testSpecific() throws Exception {
    Dao<TestRecord> dao = new SpecificAvroDao<TestRecord>(tablePool,
      tableName, schemaString, TestRecord.class);

    for (TestRecord testRecord : this.createSpecificEntities(10)) {
      assertTrue(dao.put(testRecord));
    }

    for (int i = 0; i < 10; ++i) {
      PartitionKey partitionKey = new PartitionKey("part1_" + i, "part2_" + i);
      TestRecord record = dao.get(partitionKey);
      assertEquals("field1_" + i, record.getField1());
      assertEquals("field2_" + i, record.getField2());
      assertEquals(TestEnum.ENUM3, record.getEnum$());
      assertEquals("field3_value_1_" + i,
          record.getField3().get("field3_key_1_" + i));
      assertEquals("field3_value_2_" + i,
          record.getField3().get("field3_key_2_" + i));
      assertEquals("embedded1_" + i, record.getField4().getEmbeddedField1());
      assertEquals(i, (long) record.getField4().getEmbeddedField2());
      assertEquals(2, record.getField5().size());
      // check 1st subrecord
      assertEquals("subfield1_" + i, record.getField5().get(0).getSubfield1());
      assertEquals(i, (long) record.getField5().get(0).getSubfield2());
      assertEquals("subfield3_" + i, record.getField5().get(0).getSubfield3());
      assertEquals("subfield4_" + i, record.getField5().get(1).getSubfield1());
      assertEquals(i, (long) record.getField5().get(1).getSubfield2());
      assertEquals("subfield6_" + i, record.getField5().get(1).getSubfield3());
    }

    int cnt = 0;
    EntityScanner<TestRecord> entityScanner = dao.getScanner();
    entityScanner.initialize();
    try {
      for (TestRecord entity : entityScanner) {
        assertEquals("field1_" + cnt, entity.getField1());
        assertEquals("field2_" + cnt, entity.getField2());
        cnt++;
      }
      assertEquals(10, cnt);
    } finally {
      entityScanner.close();
    }

    // Test scanner with null keys
    PartitionKey key1 = new PartitionKey("part1_5");
    entityScanner = dao.getScanner(key1, null);
    entityScanner.initialize();
    assertEquals("field1_5", entityScanner.iterator().next().getField1());

    entityScanner = dao.getScanner(null, key1);
    entityScanner.initialize();
    assertEquals("field1_0", entityScanner.iterator().next().getField1());

    PartitionKey deleteKey = new PartitionKey("part1_5", "part2_5");
    dao.delete(deleteKey);
    assertNull(dao.get(deleteKey));
  }

  @Test
  public void testIncrement() {
    Dao<TestIncrement> dao = new SpecificAvroDao<TestIncrement>(tablePool,
      incrementTableName, incrementSchemaString,
        TestIncrement.class);

    TestIncrement entity = TestIncrement.newBuilder().setKeyPart1("part1")
        .setKeyPart2("part2").setField1(10).build();
    assertTrue(dao.put(entity));

    PartitionKey key = new PartitionKey("part1", "part2");
    long incrementResult = dao.increment(key, "field1", 5);
    assertEquals(15L, incrementResult);
    assertEquals(15L, (long) dao.get(key).getField1());
  }

  @Test
  public void testConflict() throws Exception {
    Dao<TestRecord> dao = new SpecificAvroDao<TestRecord>(tablePool,
      tableName, schemaString, TestRecord.class);

    // create key and entity, and do a put
    TestRecord entity = createSpecificEntity("part1", "part2");
    assertTrue(dao.put(entity));

    // now fetch the entity twice. Change one, and do a put. Change the other,
    // and the second put should fail.
    PartitionKey key = new PartitionKey("part1", "part2");
    TestRecord recordRef1 = TestRecord.newBuilder(dao.get(key))
        .setField1("part1_1").build();
    TestRecord recordRef2 = TestRecord.newBuilder(dao.get(key))
        .setField1("part1_2").build();
    assertTrue(dao.put(recordRef1));
    assertFalse(dao.put(recordRef2));

    // Now get the latest version, change it, and put should succeed.
    recordRef2 = dao.get(key);
    assertEquals("part1_1", recordRef2.getField1());
    recordRef2 = TestRecord.newBuilder(recordRef2).setField1("part1_2").build();
    assertTrue(dao.put(recordRef2));

    // validate the most recent values.
    TestRecord finalRecord = dao.get(key);
    assertEquals("part1_2", finalRecord.getField1());

    // if we put a new entity, there should be a conflict
    assertFalse(dao.put(entity));
  }

  @Test
  public void testEmptyCollections() throws Exception {
    Dao<TestRecord> dao = new SpecificAvroDao<TestRecord>(tablePool,
      tableName, schemaString, TestRecord.class);

    Map<String, String> field3Map = new HashMap<String, String>();
    EmbeddedRecord embeddedRecord = EmbeddedRecord.newBuilder()
        .setEmbeddedField1("embedded1").setEmbeddedField2(2).build();

    TestRecord entity = TestRecord.newBuilder().setKeyPart1("part1")
        .setKeyPart2("part2").setField1("field1").setField2("field2")
        .setEnum$(TestEnum.ENUM3).setField3(field3Map)
        .setField4(embeddedRecord).setField5(new ArrayList<ArrayRecord>())
        .build();

    assertTrue(dao.put(entity));

    PartitionKey key = new PartitionKey("part1", "part2");
    TestRecord record = dao.get(key);

    assertEquals("field1", record.getField1());
    assertEquals("field2", record.getField2());
    assertEquals(TestEnum.ENUM3, record.getEnum$());
    assertEquals(0, record.getField3().size());
    assertEquals("embedded1", record.getField4().getEmbeddedField1());
    assertEquals(2L, (long) record.getField4().getEmbeddedField2());
    assertEquals(0, record.getField5().size());
  }

  /*
   * Regression test for the deleteColumn vs deleteColumns issue
   */
  @Test
  public void testDeleteAfterMultiplePuts() throws Exception {
    Dao<TestRecord> dao = new SpecificAvroDao<TestRecord>(tablePool,
      tableName, schemaString, TestRecord.class);

    for (int i = 0; i < 10; ++i) {
      TestRecord entity = createSpecificEntity("part1_" + i, "part2_" + i);
      assertTrue(dao.put(entity));
    }

    // get and put it a couple of times to build up versions
    PartitionKey key = new PartitionKey("part1_5", "part2_5");
    TestRecord entity = dao.get(key);
    dao.put(entity);
    entity = dao.get(key);
    dao.put(entity);

    // now make sure the dao removes all versions of all columns
    dao.delete(key);
    TestRecord deletedRecord = dao.get(key);
    assertNull(deletedRecord);
  }

  @Test
  public void testBatchPutOperation() throws Exception {
    Dao<TestRecord> dao = new SpecificAvroDao<TestRecord>(tablePool, tableName,
        schemaString, TestRecord.class);

    EntityBatch<TestRecord> batch = dao.newBatch();
    batch.initialize();
    for (TestRecord entity : createSpecificEntities(100)) {
      batch.put(entity);
    }
    batch.close();

    for (int i = 0; i < 100; i++) {
      PartitionKey key = new PartitionKey("part1_" + i, "part2_" + i);
      TestRecord record = dao.get(key);
      assertEquals("field1_" + i, record.getField1());
    }
  }

  @Test(expected = DatasetException.class)
  public void testPutWithNullKey() throws Exception {
    Dao<GenericRecord> dao = new GenericAvroDao(tablePool, tableName,
        schemaString);
    @SuppressWarnings("deprecation")
    GenericRecord entity = new GenericData.Record(Schema.parse(schemaString));
    entity.put("keyPart1", "part1");
    entity.put("keyPart2", null);
    entity.put("field1", "field1");
    entity.put("field2", "field2");
    dao.put(entity);
  }

  private TestRecord createSpecificEntity(String keyPart1, String keyPart2) {
    Map<String, String> field3Map = new HashMap<String, String>();
    field3Map.put("field3_key_1", "field3_value_1");
    field3Map.put("field3_key_2", "field3_value_2");

    EmbeddedRecord embeddedRecord = EmbeddedRecord.newBuilder()
        .setEmbeddedField1("embedded1").setEmbeddedField2(2).build();

    List<ArrayRecord> arrayRecordList = new ArrayList<ArrayRecord>(2);
    ArrayRecord subRecord = ArrayRecord.newBuilder().setSubfield1("subfield1")
        .setSubfield2(1L).setSubfield3("subfield3").build();
    arrayRecordList.add(subRecord);
    subRecord = ArrayRecord.newBuilder().setSubfield1("subfield4")
        .setSubfield2(1L).setSubfield3("subfield6").build();
    arrayRecordList.add(subRecord);

    TestRecord entity = TestRecord.newBuilder().setKeyPart1(keyPart1)
        .setKeyPart2(keyPart2).setField1("field1").setField2("field2")
        .setEnum$(TestEnum.ENUM3).setField3(field3Map)
        .setField4(embeddedRecord).setField5(arrayRecordList).build();
    return entity;
  }

  private List<TestRecord> createSpecificEntities(int cnt) {
    List<TestRecord> entities = new ArrayList<TestRecord>();
    for (int i = 0; i < cnt; i++) {
      Map<String, String> field3Map = new HashMap<String, String>();
      field3Map.put("field3_key_1_" + i, "field3_value_1_" + i);
      field3Map.put("field3_key_2_" + i, "field3_value_2_" + i);
      EmbeddedRecord embeddedRecord = EmbeddedRecord.newBuilder()
          .setEmbeddedField1("embedded1_" + i).setEmbeddedField2(i).build();
      List<ArrayRecord> arrayRecordList = new ArrayList<ArrayRecord>(2);
      arrayRecordList.add(ArrayRecord.newBuilder()
          .setSubfield1("subfield1_" + i).setSubfield2(i)
          .setSubfield3("subfield3_" + i).build());
      arrayRecordList.add(ArrayRecord.newBuilder()
          .setSubfield1("subfield4_" + i).setSubfield2(i)
          .setSubfield3("subfield6_" + i).build());

      TestRecord entity = TestRecord.newBuilder().setKeyPart1("part1_" + i)
          .setKeyPart2("part2_" + i).setField1("field1_" + i)
          .setField2("field2_" + i).setEnum$(TestEnum.ENUM3)
          .setField3(field3Map).setField4(embeddedRecord)
          .setField5(arrayRecordList).build();

      entities.add(entity);
    }
    return entities;
  }
}
