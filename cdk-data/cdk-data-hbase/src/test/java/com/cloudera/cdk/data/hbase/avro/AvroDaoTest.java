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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.cloudera.cdk.data.hbase.avro.impl.AvroUtils;
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

import com.cloudera.cdk.data.dao.Dao;
import com.cloudera.cdk.data.dao.EntityBatch;
import com.cloudera.cdk.data.dao.KeyEntity;
import com.cloudera.cdk.data.dao.EntityScanner;
import com.cloudera.cdk.data.dao.KeyBuildException;
import com.cloudera.cdk.data.dao.PartialKey;
import com.cloudera.cdk.data.hbase.avro.entities.ArrayRecord;
import com.cloudera.cdk.data.hbase.avro.entities.EmbeddedRecord;
import com.cloudera.cdk.data.hbase.avro.entities.TestEnum;
import com.cloudera.cdk.data.hbase.avro.entities.TestKey;
import com.cloudera.cdk.data.hbase.avro.entities.TestRecord;
import com.cloudera.cdk.data.hbase.testing.HBaseTestUtils;

public class AvroDaoTest {

  private static final String keyString;
  private static final String recordString;
  private static final String tableName = "testtable";
  private HTablePool tablePool;

  static {
    try {
      keyString = AvroUtils.inputStreamToString(AvroDaoTest.class
          .getResourceAsStream("/TestKey.avsc"));
      recordString = AvroUtils.inputStreamToString(AvroDaoTest.class
          .getResourceAsStream("/TestRecord.avsc"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    HBaseTestUtils.getMiniCluster();
    byte[] tableNameBytes = Bytes.toBytes(tableName);
    byte[][] cfNames = { Bytes.toBytes("meta"), Bytes.toBytes("string"),
        Bytes.toBytes("embedded"), Bytes.toBytes("_s") };
    HBaseTestUtils.util.createTable(tableNameBytes, cfNames);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HBaseTestUtils.util.deleteTable(Bytes.toBytes(tableName));
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
    Dao<GenericRecord, GenericRecord> dao = new GenericAvroDao(tablePool,
        tableName, keyString, recordString);

    for (int i = 0; i < 10; ++i) {
      GenericRecord key = createGenericKey("part1_" + Integer.toString(i),
          "part2_" + Integer.toString(i));
      @SuppressWarnings("deprecation")
      GenericRecord entity = new GenericData.Record(Schema.parse(recordString));
      entity.put("field1", "field1_" + Integer.toString(i));
      entity.put("field2", "field2_" + Integer.toString(i));
      dao.put(key, entity);
    }

    for (int i = 0; i < 10; ++i) {
      GenericRecord key = createGenericKey("part1_" + Integer.toString(i),
          "part2_" + Integer.toString(i));
      GenericRecord genericRecord = dao.get(key);
      assertEquals("field1_" + Integer.toString(i), genericRecord.get("field1")
          .toString());
      assertEquals("field2_" + Integer.toString(i), genericRecord.get("field2")
          .toString());
    }

    int cnt = 0;
    EntityScanner<GenericRecord, GenericRecord> entityScanner = dao
        .getScanner();
    try {
      for (KeyEntity<GenericRecord, GenericRecord> keyEntity : entityScanner) {
        assertEquals("part1_" + Integer.toString(cnt),
            keyEntity.getKey().get("part1").toString());
        assertEquals("part2_" + Integer.toString(cnt),
            keyEntity.getKey().get("part2").toString());
        assertEquals("field1_" + Integer.toString(cnt), keyEntity.getEntity()
            .get("field1").toString());
        assertEquals("field2_" + Integer.toString(cnt), keyEntity.getEntity()
            .get("field2").toString());
        cnt++;
      }
      assertEquals(10, cnt);
    } finally {
      entityScanner.close();
    }

    cnt = 5;
    PartialKey<GenericRecord> startKey = new PartialKey.Builder<GenericRecord>()
        .addKeyPart("part1", "part1_5").build();
    entityScanner = dao.getScanner(startKey, null);
    try {
      for (KeyEntity<GenericRecord, GenericRecord> keyEntity : entityScanner) {
        assertEquals("part1_" + Integer.toString(cnt),
            keyEntity.getKey().get("part1").toString());
        assertEquals("part2_" + Integer.toString(cnt),
            keyEntity.getKey().get("part2").toString());
        assertEquals("field1_" + Integer.toString(cnt), keyEntity.getEntity()
            .get("field1").toString());
        assertEquals("field2_" + Integer.toString(cnt), keyEntity.getEntity()
            .get("field2").toString());
        cnt++;
      }
      assertEquals(10, cnt);
    } finally {
      if (entityScanner != null) {
        entityScanner.close();
      }
    }

    GenericRecord key = createGenericKey("part1_5", "part2_5");
    dao.delete(key);
    GenericRecord deletedRecord = dao.get(key);
    assertNull(deletedRecord);
  }

  @Test
  public void testSpecific() throws Exception {
    Dao<TestKey, TestRecord> dao = new SpecificAvroDao<TestKey, TestRecord>(
        tablePool, new String(tableName), keyString, recordString,
        TestKey.class, TestRecord.class);

    int cnt = 0;
    for (TestRecord testRecord : this.createSpecificEntities(10)) {
      TestKey keyRecord = this.createSpecificKey(
          "part1_" + Integer.toString(cnt), "part2_" + Integer.toString(cnt));
      assertTrue(dao.put(keyRecord, testRecord));
      cnt++;
    }

    for (int i = 0; i < 10; ++i) {
      String iStr = Integer.toString(i);
      TestKey key = createSpecificKey("part1_" + iStr, "part2_" + iStr);

      TestRecord record = dao.get(key);
      assertEquals("field1_" + iStr, record.getField1());
      assertEquals("field2_" + iStr, record.getField2());
      assertEquals(TestEnum.ENUM3, record.getEnum$());
      assertEquals("field3_value_1_" + iStr,
          record.getField3().get("field3_key_1_" + iStr));
      assertEquals("field3_value_2_" + iStr,
          record.getField3().get("field3_key_2_" + iStr));
      assertEquals("embedded1_" + iStr, record.getField4().getEmbeddedField1());
      assertEquals(i, (long) record.getField4().getEmbeddedField2());
      assertEquals(2, record.getField5().size());
      // check 1st subrecord
      assertEquals("subfield1_" + iStr, record.getField5().get(0)
          .getSubfield1());
      assertEquals(i, (long) record.getField5().get(0).getSubfield2());
      assertEquals("subfield3_" + iStr, record.getField5().get(0)
          .getSubfield3());
      assertEquals("subfield4_" + iStr, record.getField5().get(1)
          .getSubfield1());
      assertEquals(i, (long) record.getField5().get(1).getSubfield2());
      assertEquals("subfield6_" + iStr, record.getField5().get(1)
          .getSubfield3());
    }

    cnt = 0;
    EntityScanner<TestKey, TestRecord> entityScanner = dao.getScanner();
    try {
      for (KeyEntity<TestKey, TestRecord> keyEntity : entityScanner) {
        TestKey key = keyEntity.getKey();
        assertEquals("part1_" + Integer.toString(cnt), key.getPart1());
        assertEquals("part2_" + Integer.toString(cnt), key.getPart2());
        assertEquals("field1_" + Integer.toString(cnt), keyEntity.getEntity()
            .getField1());
        assertEquals("field2_" + Integer.toString(cnt), keyEntity.getEntity()
            .getField2());
        cnt++;
      }
      assertEquals(10, cnt);
    } finally {
      entityScanner.close();
    }

    // Test scanner with null keys
    PartialKey<TestKey> key1 = new PartialKey.Builder<TestKey>().addKeyPart(
        "part1", "part1_5").build();
    entityScanner = dao.getScanner(key1, null);
    assertEquals("field1_5", entityScanner.iterator().next().getEntity()
        .getField1());

    key1 = new PartialKey.Builder<TestKey>().addKeyPart("part1", "part1_5")
        .build();
    entityScanner = dao.getScanner(null, key1);
    assertEquals("field1_0", entityScanner.iterator().next().getEntity()
        .getField1());

    TestKey key = createSpecificKey("part1_5", "part2_5");
    dao.delete(key);
    assertNull(dao.get(key));
  }

  @Test(expected = KeyBuildException.class)
  public void testInvalidPartialScan() {
    Dao<TestKey, TestRecord> dao = new SpecificAvroDao<TestKey, TestRecord>(
        tablePool, new String(tableName), keyString, recordString,
        TestKey.class, TestRecord.class);

    // Test partial key scan with second part of key built
    PartialKey<TestKey> key = new PartialKey.Builder<TestKey>().addKeyPart(
        "part2", "part1_5").build();

    // should throw a KeyBuildException
    dao.getScanner(key, null);
  }

  @Test
  public void testIncrement() {
    Dao<TestKey, TestRecord> dao = new SpecificAvroDao<TestKey, TestRecord>(
        tablePool, new String(tableName), keyString, recordString,
        TestKey.class, TestRecord.class);

    TestKey keyRecord = TestKey.newBuilder().setPart1("part1")
        .setPart2("part2").build();

    Map<String, String> field3Map = new HashMap<String, String>();
    field3Map.put("field3_key_1", "field3_value_1");
    field3Map.put("field3_key_2", "field3_value_2");

    EmbeddedRecord embeddedRecord = EmbeddedRecord.newBuilder()
        .setEmbeddedField1("embedded1").setEmbeddedField2(2).build();

    TestRecord entity = TestRecord.newBuilder().setField1("field1")
        .setField2("field2").setEnum$(TestEnum.ENUM3).setField3(field3Map)
        .setField4(embeddedRecord).setField5(new ArrayList<ArrayRecord>())
        .setIncrement(10).build();

    assertTrue(dao.put(keyRecord, entity));

    long incrementResult = dao.increment(keyRecord, "increment", 5);
    assertEquals(15L, incrementResult);
    assertEquals(15L, (long) dao.get(keyRecord).getIncrement());
  }

  @Test
  public void testConflict() throws Exception {
    Dao<TestKey, TestRecord> dao = new SpecificAvroDao<TestKey, TestRecord>(
        tablePool, new String(tableName), keyString, recordString,
        TestKey.class, TestRecord.class);

    // create key and entity, and do a put
    TestKey keyRecord = createSpecificKey("part1", "part2");
    TestRecord entity = createSpecificEntity();
    assertTrue(dao.put(keyRecord, entity));

    // now fetch the entity twice. Change one, and do a put. Change the other,
    // and the second put should fail.
    TestRecord recordRef1 = TestRecord.newBuilder(dao.get(keyRecord))
        .setField1("part1_1").build();
    TestRecord recordRef2 = TestRecord.newBuilder(dao.get(keyRecord))
        .setField1("part1_2").build();
    assertTrue(dao.put(keyRecord, recordRef1));
    assertFalse(dao.put(keyRecord, recordRef2));

    // Now get the latest version, change it, and put should succeed.
    recordRef2 = dao.get(keyRecord);
    assertEquals("part1_1", recordRef2.getField1());
    recordRef2 = TestRecord.newBuilder(recordRef2).setField1("part1_2").build();
    assertTrue(dao.put(keyRecord, recordRef2));

    // validate the most recent values.
    TestRecord finalRecord = dao.get(keyRecord);
    assertEquals("part1_2", finalRecord.getField1());

    // if we put a new entity, there should be a conflict
    assertFalse(dao.put(keyRecord, entity));
  }

  @Test
  public void testEmptyCollections() throws Exception {
    Dao<TestKey, TestRecord> dao = new SpecificAvroDao<TestKey, TestRecord>(
        tablePool, new String(tableName), keyString, recordString,
        TestKey.class, TestRecord.class);

    TestKey keyRecord = TestKey.newBuilder().setPart1("part1")
        .setPart2("part2").build();

    Map<String, String> field3Map = new HashMap<String, String>();
    EmbeddedRecord embeddedRecord = EmbeddedRecord.newBuilder()
        .setEmbeddedField1("embedded1").setEmbeddedField2(2).build();

    TestRecord entity = TestRecord.newBuilder().setField1("field1")
        .setField2("field2").setEnum$(TestEnum.ENUM3).setField3(field3Map)
        .setField4(embeddedRecord).setField5(new ArrayList<ArrayRecord>())
        .setIncrement(10).build();

    assertTrue(dao.put(keyRecord, entity));

    TestKey key = createSpecificKey("part1", "part2");
    TestRecord record = dao.get(key);

    assertEquals("field1", record.getField1());
    assertEquals("field2", record.getField2());
    assertEquals(TestEnum.ENUM3, record.getEnum$());
    assertEquals(0, record.getField3().size());
    assertEquals("embedded1", record.getField4().getEmbeddedField1());
    assertEquals(2L, (long) record.getField4().getEmbeddedField2());
    assertEquals(0, record.getField5().size());
    assertEquals(10L, (long) record.getIncrement());
  }

  /*
   * Regression test for the deleteColumn vs deleteColumns issue
   */
  @Test
  public void testDeleteAfterMultiplePuts() throws Exception {
    Dao<TestKey, TestRecord> dao = new SpecificAvroDao<TestKey, TestRecord>(
        tablePool, new String(tableName), keyString, recordString,
        TestKey.class, TestRecord.class);

    for (int i = 0; i < 10; ++i) {
      String iStr = Integer.toString(i);
      TestKey keyRecord = createSpecificKey("part1_" + iStr, "part2_" + iStr);
      TestRecord entity = createSpecificEntity();
      assertTrue(dao.put(keyRecord, entity));
    }

    // get and put it a couple of times to build up versions
    TestKey key = createSpecificKey("part1_5", "part2_5");
    TestRecord entity = dao.get(key);
    dao.put(key, entity);
    entity = dao.get(key);
    dao.put(key, entity);

    // now make sure the dao removes all versions of all columns
    dao.delete(key);
    TestRecord deletedRecord = dao.get(key);
    assertNull(deletedRecord);
  }

  @Test
  public void testBatchPutOperation() throws Exception {
    Dao<TestKey, TestRecord> dao = new SpecificAvroDao<TestKey, TestRecord>(
        tablePool, tableName, keyString, recordString, TestKey.class,
        TestRecord.class);

    EntityBatch<TestKey, TestRecord> batch = dao.newBatch();

    int cnt = 0;
    for (TestRecord entity : createSpecificEntities(100)) {
      TestKey keyRecord = createSpecificKey("part1_" + cnt, "part2_" + cnt);
      batch.put(keyRecord, entity);
      cnt++;
    }
    batch.close();

    for (int i = 0; i < 100; i++) {
      String iStr = Integer.toString(i);
      TestKey keyRecord = createSpecificKey("part1_" + iStr, "part2_" + iStr);
      TestRecord record = dao.get(keyRecord);
      assertEquals("field1_" + iStr, record.getField1().toString());
    }
  }

  private GenericRecord createGenericKey(String part1Value, String part2Value) {
    Schema.Parser parser = new Schema.Parser();
    GenericRecord key = new GenericData.Record(parser.parse(keyString));
    key.put("part1", part1Value);
    key.put("part2", part2Value);
    return key;
  }

  private TestKey createSpecificKey(String part1Value, String part2Value) {
    return TestKey.newBuilder().setPart1(part1Value).setPart2(part2Value)
        .build();
  }

  private TestRecord createSpecificEntity() {
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

    TestRecord entity = TestRecord.newBuilder().setField1("field1")
        .setField2("field2").setEnum$(TestEnum.ENUM3).setField3(field3Map)
        .setField4(embeddedRecord).setField5(arrayRecordList).setIncrement(10)
        .build();
    return entity;
  }

  private List<TestRecord> createSpecificEntities(int cnt) {
    List<TestRecord> entities = new ArrayList<TestRecord>();
    for (int i = 0; i < cnt; i++) {
      String iterStr = Integer.toString(i);
      Map<String, String> field3Map = new HashMap<String, String>();
      field3Map.put("field3_key_1_" + iterStr, "field3_value_1_" + iterStr);
      field3Map.put("field3_key_2_" + iterStr, "field3_value_2_" + iterStr);
      EmbeddedRecord embeddedRecord = EmbeddedRecord.newBuilder()
          .setEmbeddedField1("embedded1_" + iterStr).setEmbeddedField2(i)
          .build();
      List<ArrayRecord> arrayRecordList = new ArrayList<ArrayRecord>(2);
      arrayRecordList.add(ArrayRecord.newBuilder()
          .setSubfield1("subfield1_" + iterStr).setSubfield2(i)
          .setSubfield3("subfield3_" + iterStr).build());
      arrayRecordList.add(ArrayRecord.newBuilder()
          .setSubfield1("subfield4_" + iterStr).setSubfield2(i)
          .setSubfield3("subfield6_" + iterStr).build());

      TestRecord entity = TestRecord.newBuilder()
          .setField1("field1_" + iterStr).setField2("field2_" + iterStr)
          .setEnum$(TestEnum.ENUM3).setField3(field3Map)
          .setField4(embeddedRecord).setField5(arrayRecordList).setIncrement(i)
          .build();

      entities.add(entity);
    }
    return entities;
  }
}
