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
package org.kitesdk.data.hbase;

import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Key;
import org.kitesdk.data.RandomAccessDataset;
import org.kitesdk.data.hbase.avro.AvroUtils;
import org.kitesdk.data.hbase.avro.entities.ArrayRecord;
import org.kitesdk.data.hbase.avro.entities.EmbeddedRecord;
import org.kitesdk.data.hbase.avro.entities.TestEntity;
import org.kitesdk.data.hbase.avro.entities.TestEnum;
import org.kitesdk.data.hbase.testing.HBaseTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class HBaseDatasetRepositoryTest {

  private static final String testEntity;
  private static final String testGenericEntity;
  private static final String testGenericEntity2;
  private static final String tableName = "testtable";
  private static final String managedTableName = "managed_schemas";

  static {
    try {
      testEntity = AvroUtils.inputStreamToString(HBaseDatasetRepositoryTest.class
          .getResourceAsStream("/TestEntity.avsc"));
      testGenericEntity = AvroUtils.inputStreamToString(HBaseDatasetRepositoryTest.class
          .getResourceAsStream("/TestGenericEntity.avsc"));
      testGenericEntity2 = AvroUtils.inputStreamToString(HBaseDatasetRepositoryTest.class
          .getResourceAsStream("/TestGenericEntity2.avsc"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    HBaseTestUtils.getMiniCluster();
    // managed table should be created by HBaseDatasetRepository
    HBaseTestUtils.util.deleteTable(Bytes.toBytes(managedTableName));
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
  @SuppressWarnings("unchecked")
  public void testGeneric() throws Exception {
    String datasetName = tableName + ".TestGenericEntity";
    HBaseDatasetRepository repo = new HBaseDatasetRepository.Builder()
        .configuration(HBaseTestUtils.getConf()).build();
    
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral(testGenericEntity)
        .build();
    DaoDataset<GenericRecord> ds = (DaoDataset) repo.create("default", datasetName, descriptor);

    // Create the new entities
    ds.put(createGenericEntity(0));
    ds.put(createGenericEntity(1));

    DatasetWriter<GenericRecord> writer = ds.newWriter();
    assertTrue("Writer should be open initially", writer.isOpen());
    try {
      for (int i = 2; i < 10; ++i) {
        GenericRecord entity = createGenericEntity(i);
        writer.write(entity);
      }
    } finally {
      writer.close();
      assertFalse("Writer should be closed after calling close", writer.isOpen());
    }

    // reload
    ds = (DaoDataset) repo.load("default", datasetName);

    // ensure the new entities are what we expect with get operations
    for (int i = 0; i < 10; ++i) {
      String iStr = Long.toString(i);
      Key key = new Key.Builder(ds)
          .add("part1", new Utf8("part1_" + iStr))
          .add("part2", new Utf8("part2_" + iStr)).build();
      compareEntitiesWithUtf8(i, ds.get(key));
    }

    // ensure the new entities are what we expect with scan operations
    int cnt = 0;
    DatasetReader<GenericRecord> reader = ds.newReader();
    assertTrue("Reader should be open initially", reader.isOpen());
    try {
      for (GenericRecord entity : reader) {
        compareEntitiesWithUtf8(cnt, entity);
        cnt++;
      }
      assertEquals(10, cnt);
    } finally {
      reader.close();
      assertFalse("Reader should be closed after calling close", reader.isOpen());
    }

    // test a partial scan
    cnt = 3;
    reader = new DaoView<GenericRecord>(ds, GenericRecord.class)
        .from("part1", new Utf8("part1_3")).from("part2", new Utf8("part2_3"))
        .to("part1", new Utf8("part1_7")).to("part2", new Utf8("part2_7"))
        .newReader();
    try {
      for (GenericRecord entity : reader) {
        compareEntitiesWithUtf8(cnt, entity);
        cnt++;
      }
      assertEquals(8, cnt);
    } finally {
      reader.close();
    }

    Key key = new Key.Builder(ds)
        .add("part1", new Utf8("part1_5"))
        .add("part2", new Utf8("part2_5")).build();

    // test delete
    ds.delete(key);
    GenericRecord deletedRecord = ds.get(key);
    assertNull(deletedRecord);
  }

  @Test
  public void testSpecific() throws Exception {
    String datasetName = tableName + ".TestEntity";
    HBaseDatasetRepository repo = new HBaseDatasetRepository.Builder()
        .configuration(HBaseTestUtils.getConf()).build();

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral(testEntity)
        .build();
    RandomAccessDataset<TestEntity> ds = repo.create("default", datasetName, descriptor);

    // Create the new entities
    ds.put(createSpecificEntity(0));
    ds.put(createSpecificEntity(1));

    DatasetWriter<TestEntity> writer = ds.newWriter();
    try {
      for (int i = 2; i < 10; ++i) {
        TestEntity entity = createSpecificEntity(i);
        writer.write(entity);
      }
    } finally {
      writer.close();
    }

    // ensure the new entities are what we expect with get operations
    for (int i = 0; i < 10; ++i) {
      String iStr = Long.toString(i);
      Key key = new Key.Builder(ds)
          .add("part1", "part1_" + iStr)
          .add("part2", "part2_" + iStr).build();
      compareEntitiesWithString(i, ds.get(key));
    }

    // ensure the new entities are what we expect with scan operations
    int cnt = 0;
    DatasetReader<TestEntity> reader = ds.newReader();
    try {
      for (TestEntity entity : reader) {
        compareEntitiesWithString(cnt, entity);
        cnt++;
      }
      assertEquals(10, cnt);
    } finally {
      reader.close();
    }

    Key key = new Key.Builder(ds)
        .add("part1", "part1_5")
        .add("part2", "part2_5").build();

    // test delete
    ds.delete(key);
    TestEntity deletedRecord = ds.get(key);
    assertNull(deletedRecord);
  }

  @Test
  public void testDeleteDataset() throws Exception {

    String datasetName = tableName + ".TestGenericEntity";
    HBaseDatasetRepository repo = new HBaseDatasetRepository.Builder()
        .configuration(HBaseTestUtils.getConf()).build();

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral(testGenericEntity)
        .build();
    RandomAccessDataset<GenericRecord> ds = repo.create("default", datasetName, descriptor);

    // Create a new entity
    ds.put(createGenericEntity(0));

    // Retrieve the entity
    String iStr = Long.toString(0);
    Key key = new Key.Builder(ds)
        .add("part1", "part1_" + iStr)
        .add("part2", "part2_" + iStr).build();
    compareEntitiesWithUtf8(0, ds.get(key));

    // delete dataset
    boolean success = repo.delete("default", datasetName);
    assertTrue("dataset should have been successfully deleted", success);

    assertFalse("second delete should return false", repo.delete("default", datasetName));

    // check that tables have no rows
    assertEquals(0, HBaseTestUtils.util.countRows(new HTable(HBaseTestUtils.getConf(), managedTableName)));
    assertEquals(0, HBaseTestUtils.util.countRows(new HTable(HBaseTestUtils.getConf(), tableName)));

    // create the dataset again
    ds = repo.create("default", datasetName, descriptor);

    // Create a new entity
    ds.put(createGenericEntity(0));

    // Retrieve the entity
    compareEntitiesWithUtf8(0, ds.get(key));
  }

  @Test
  public void testUpdateDataset() throws Exception {

    String datasetName = tableName + ".TestGenericEntity";

    HBaseDatasetRepository repo = new HBaseDatasetRepository.Builder()
        .configuration(HBaseTestUtils.getConf()).build();

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral(testGenericEntity)
        .build();
    RandomAccessDataset<GenericRecord> ds = repo.create("default", datasetName, descriptor);

    // Create a new entity
    ds.put(createGenericEntity(0));

    DatasetDescriptor newDescriptor = new DatasetDescriptor.Builder()
        .schemaLiteral(testGenericEntity2)
        .build();
    repo.update("default", datasetName, newDescriptor);
  }

  // TODO: remove duplication from ManagedDaoTest

  public static GenericRecord createGenericEntity(long uniqueIdx) {
    return createGenericEntity(uniqueIdx, testGenericEntity);
  }

  /**
   * Creates a generic entity with the given schema, which must be migratable
   * from TestRecord. Data from TestRecord is filled in.
   */
  private static GenericRecord createGenericEntity(long uniqueIdx, String schemaString) {
    String iStr = Long.toString(uniqueIdx);

    Schema.Parser parser = new Schema.Parser();
    Schema entitySchema = parser.parse(schemaString);
    Schema embeddedSchema = entitySchema.getField("field4").schema();
    Schema arrayValueSchema = entitySchema.getField("field5").schema()
        .getElementType();

    GenericRecord entity = new GenericData.Record(entitySchema);

    entity.put("part1", "part1_" + iStr);
    entity.put("part2", "part2_" + iStr);

    entity.put("field1", "field1_" + iStr);
    entity.put("field2", "field2_" + iStr);
    entity.put("enum", "ENUM3");
    Map<CharSequence, CharSequence> field3Map = new HashMap<CharSequence, CharSequence>();
    field3Map.put("field3_key_1_" + iStr, "field3_value_1_" + iStr);
    field3Map.put("field3_key_2_" + iStr, "field3_value_2_" + iStr);
    entity.put("field3", field3Map);
    GenericRecord embedded = new GenericData.Record(embeddedSchema);
    embedded.put("embeddedField1", "embedded1");
    embedded.put("embeddedField2", 2L);
    entity.put("field4", embedded);

    List<GenericRecord> arrayRecordList = new ArrayList<GenericRecord>();
    GenericRecord subRecord = new GenericData.Record(arrayValueSchema);
    subRecord.put("subfield1", "subfield1");
    subRecord.put("subfield2", 1L);
    subRecord.put("subfield3", "subfield3");
    arrayRecordList.add(subRecord);

    subRecord = new GenericData.Record(arrayValueSchema);
    subRecord.put("subfield1", "subfield4");
    subRecord.put("subfield2", 1L);
    subRecord.put("subfield3", "subfield6");
    arrayRecordList.add(subRecord);

    entity.put("field5", arrayRecordList);

    return entity;
  }

  @SuppressWarnings("unchecked")
  public static void compareEntitiesWithUtf8(long uniqueIdx, IndexedRecord record) {
    String iStr = Long.toString(uniqueIdx);

    assertEquals("part1_" + iStr, record.get(0).toString()); // TODO: check type
    assertEquals("part2_" + iStr, record.get(1).toString()); // TODO: check type

    assertEquals(new Utf8("field1_" + iStr), record.get(2));
    assertEquals(new Utf8("field2_" + iStr), record.get(3));
    assertEquals(TestEnum.ENUM3.toString(), record.get(4).toString());
    assertEquals(new Utf8("field3_value_1_" + iStr),
        ((Map<CharSequence, CharSequence>) record.get(5)).get(new Utf8(
            "field3_key_1_" + iStr)));
    assertEquals(new Utf8("field3_value_2_" + iStr),
        ((Map<CharSequence, CharSequence>) record.get(5)).get(new Utf8(
            "field3_key_2_" + iStr)));
    assertEquals(new Utf8("embedded1"), ((IndexedRecord) record.get(6)).get(0));
    assertEquals(2L, ((IndexedRecord) record.get(6)).get(1));

    assertEquals(2, ((List<?>) record.get(7)).size());
    assertEquals(new Utf8("subfield1"),
        ((IndexedRecord) ((List<?>) record.get(7)).get(0)).get(0));
    assertEquals(1L, ((IndexedRecord) ((List<?>) record.get(7)).get(0)).get(1));
    assertEquals(new Utf8("subfield3"),
        ((IndexedRecord) ((List<?>) record.get(7)).get(0)).get(2));
    assertEquals(new Utf8("subfield4"),
        ((IndexedRecord) ((List<?>) record.get(7)).get(1)).get(0));
    assertEquals(1L, ((IndexedRecord) ((List<?>) record.get(7)).get(1)).get(1));
    assertEquals(new Utf8("subfield6"),
        ((IndexedRecord) ((List<?>) record.get(7)).get(1)).get(2));
  }

  @SuppressWarnings("unchecked")
  private void compareEntitiesWithString(long uniqueIdx, IndexedRecord record) {
    String iStr = Long.toString(uniqueIdx);

    assertEquals("part1_" + iStr, record.get(0).toString()); // TODO: check type
    assertEquals("part2_" + iStr, record.get(1).toString()); // TODO: check type

    assertEquals("field1_" + iStr, record.get(2));
    assertEquals("field2_" + iStr, record.get(3));
    assertEquals(TestEnum.ENUM3.toString(), record.get(4).toString());
    assertEquals(
        "field3_value_1_" + iStr,
        ((Map<CharSequence, CharSequence>) record.get(5)).get("field3_key_1_"
            + iStr));
    assertEquals(
        "field3_value_2_" + iStr,
        ((Map<CharSequence, CharSequence>) record.get(5)).get("field3_key_2_"
            + iStr));
    assertEquals("embedded1", ((IndexedRecord) record.get(6)).get(0));
    assertEquals(2L, ((IndexedRecord) record.get(6)).get(1));

    assertEquals(2, ((List<?>) record.get(7)).size());
    assertEquals("subfield1",
        ((IndexedRecord) ((List<?>) record.get(7)).get(0)).get(0));
    assertEquals(1L, ((IndexedRecord) ((List<?>) record.get(7)).get(0)).get(1));
    assertEquals("subfield3",
        ((IndexedRecord) ((List<?>) record.get(7)).get(0)).get(2));
    assertEquals("subfield4",
        ((IndexedRecord) ((List<?>) record.get(7)).get(1)).get(0));
    assertEquals(1L, ((IndexedRecord) ((List<?>) record.get(7)).get(1)).get(1));
    assertEquals("subfield6",
        ((IndexedRecord) ((List<?>) record.get(7)).get(1)).get(2));
  }

  private TestEntity createSpecificEntity(long uniqueIdx) {
    String iStr = Long.toString(uniqueIdx);

    Map<String, String> field3Map = new HashMap<String, String>();
    field3Map.put("field3_key_1_" + iStr, "field3_value_1_" + iStr);
    field3Map.put("field3_key_2_" + iStr, "field3_value_2_" + iStr);

    EmbeddedRecord embeddedRecord = EmbeddedRecord.newBuilder()
        .setEmbeddedField1("embedded1").setEmbeddedField2(2L).build();

    List<ArrayRecord> arrayRecordList = new ArrayList<ArrayRecord>(2);
    ArrayRecord subRecord = ArrayRecord.newBuilder().setSubfield1("subfield1")
        .setSubfield2(1L).setSubfield3("subfield3").build();
    arrayRecordList.add(subRecord);
    subRecord = ArrayRecord.newBuilder().setSubfield1("subfield4")
        .setSubfield2(1L).setSubfield3("subfield6").build();
    arrayRecordList.add(subRecord);

    return TestEntity.newBuilder()
        .setPart1("part1_" + iStr).setPart2("part2_" + iStr)
        .setField1("field1_" + iStr)
        .setField2("field2_" + iStr).setEnum$(TestEnum.ENUM3)
        .setField3(field3Map).setField4(embeddedRecord)
        .setField5(arrayRecordList).build();
  }
}
