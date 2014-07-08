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

import org.kitesdk.data.IncompatibleSchemaException;
import org.kitesdk.data.spi.PartitionKey;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.SchemaNotFoundException;
import org.kitesdk.data.hbase.avro.entities.ArrayRecord;
import org.kitesdk.data.hbase.avro.entities.CompositeRecord;
import org.kitesdk.data.hbase.avro.entities.EmbeddedRecord;
import org.kitesdk.data.hbase.avro.entities.SubRecord1;
import org.kitesdk.data.hbase.avro.entities.SubRecord2;
import org.kitesdk.data.hbase.avro.entities.TestEnum;
import org.kitesdk.data.hbase.avro.entities.TestIncrement;
import org.kitesdk.data.hbase.avro.entities.TestRecord;
import org.kitesdk.data.hbase.impl.Dao;
import org.kitesdk.data.hbase.impl.EntityScanner;
import org.kitesdk.data.hbase.impl.SchemaManager;
import org.kitesdk.data.hbase.manager.DefaultSchemaManager;
import org.kitesdk.data.hbase.manager.generated.ManagedSchema;
import org.kitesdk.data.hbase.testing.HBaseTestUtils;
import org.kitesdk.data.hbase.tool.SchemaTool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class ManagedDaoTest {

  private static String testRecord;
  private static String testRecordv2;
  private static String compositeSubrecord1;
  private static String compositeSubrecord2;
  private static String testIncrement;
  private static String badCreateIncompatibleColumn1;
  private static String badCreateIncompatibleColumn2;
  private static String badCreateIncompatibleColumn3;
  private static String badCreateIncompatibleKey1;
  private static String badCreateIncompatibleKey2;
  private static String badMigrationRecordAddKeyField;
  private static String badMigrationRecordAddFieldNoDefault;
  private static String badMigrationRecordAddSubFieldNoDefault;
  private static String badMigrationRecordModifiedMapping;
  private static String badMigrationRecordIntToLong;
  private static String goodMigrationRecordAddField;
  private static String goodMigrationRecordAddSubField;
  private static String goodMigrationRecordRemoveField;
  private static String managedRecordString;
  private static final String tableName = "test_table";
  private static final String compositeTableName = "test_composite_table";
  private static final String incrementTableName = "test_increment_table";
  private static final String managedTableName = "managed_schemas";

  private HTablePool tablePool;
  private SchemaManager manager;

  @BeforeClass
  public static void beforeClass() throws Exception {
    testRecord = AvroUtils.inputStreamToString(AvroDaoTest.class
        .getResourceAsStream("/TestRecord.avsc"));
    testRecordv2 = AvroUtils.inputStreamToString(AvroDaoTest.class
        .getResourceAsStream("/TestRecordv2.avsc"));
    compositeSubrecord1 = AvroUtils.inputStreamToString(AvroDaoTest.class
        .getResourceAsStream("/SubRecord1.avsc"));
    compositeSubrecord2 = AvroUtils.inputStreamToString(AvroDaoTest.class
        .getResourceAsStream("/SubRecord2.avsc"));
    testIncrement = AvroUtils.inputStreamToString(AvroDaoTest.class
        .getResourceAsStream("/TestIncrement.avsc"));
    badCreateIncompatibleColumn1 = AvroUtils
        .inputStreamToString(AvroDaoTest.class
            .getResourceAsStream("/BadCreateIncompatibleColumn1.avsc"));
    badCreateIncompatibleColumn2 = AvroUtils
        .inputStreamToString(AvroDaoTest.class
            .getResourceAsStream("/BadCreateIncompatibleColumn2.avsc"));
    badCreateIncompatibleColumn3 = AvroUtils
        .inputStreamToString(AvroDaoTest.class
            .getResourceAsStream("/BadCreateIncompatibleColumn3.avsc"));
    badCreateIncompatibleKey1 = AvroUtils
        .inputStreamToString(AvroDaoTest.class
            .getResourceAsStream("/BadCreateIncompatibleKey1.avsc"));
    badCreateIncompatibleKey2 = AvroUtils
        .inputStreamToString(AvroDaoTest.class
            .getResourceAsStream("/BadCreateIncompatibleKey2.avsc"));
    badMigrationRecordAddKeyField = AvroUtils
        .inputStreamToString(AvroDaoTest.class
            .getResourceAsStream("/BadMigrationRecordAddKeyField.avsc"));
    badMigrationRecordAddFieldNoDefault = AvroUtils
        .inputStreamToString(AvroDaoTest.class
            .getResourceAsStream("/BadMigrationRecordAddFieldNoDefault.avsc"));
    badMigrationRecordAddSubFieldNoDefault = AvroUtils
        .inputStreamToString(AvroDaoTest.class
            .getResourceAsStream("/BadMigrationRecordAddSubFieldNoDefault.avsc"));
    badMigrationRecordModifiedMapping = AvroUtils
        .inputStreamToString(AvroDaoTest.class
            .getResourceAsStream("/BadMigrationRecordModifiedMapping.avsc"));
    badMigrationRecordIntToLong = AvroUtils
        .inputStreamToString(AvroDaoTest.class
            .getResourceAsStream("/BadMigrationRecordIntToLong.avsc"));
    goodMigrationRecordAddField = AvroUtils
        .inputStreamToString(AvroDaoTest.class
            .getResourceAsStream("/GoodMigrationRecordAddField.avsc"));
    goodMigrationRecordAddSubField = AvroUtils
        .inputStreamToString(AvroDaoTest.class
            .getResourceAsStream("/GoodMigrationRecordAddSubField.avsc"));
    goodMigrationRecordRemoveField = AvroUtils
        .inputStreamToString(AvroDaoTest.class
            .getResourceAsStream("/GoodMigrationRecordRemoveField.avsc"));
    managedRecordString = AvroUtils.inputStreamToString(AvroDaoTest.class
        .getResourceAsStream("/ManagedSchema.avsc"));
    
    HBaseTestUtils.getMiniCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HBaseTestUtils.util.deleteTable(Bytes.toBytes(tableName));
    HBaseTestUtils.util.deleteTable(Bytes.toBytes(incrementTableName));
  }

  @Before
  public void before() throws Exception {
    tablePool = new HTablePool(HBaseTestUtils.getConf(), 10);
    SchemaTool tool = new SchemaTool(new HBaseAdmin(HBaseTestUtils.getConf()),
        new DefaultSchemaManager(tablePool));
    tool.createOrMigrateSchema(tableName, testRecord, true);
    tool.createOrMigrateSchema(tableName, testRecordv2, true);
    tool.createOrMigrateSchema(compositeTableName, compositeSubrecord1, true);
    tool.createOrMigrateSchema(compositeTableName, compositeSubrecord2, true);
    tool.createOrMigrateSchema(incrementTableName, testIncrement, true);
    manager = new DefaultSchemaManager(tablePool);
  }

  @After
  public void after() throws Exception {
    tablePool.close();
    HBaseTestUtils.util.truncateTable(Bytes.toBytes(tableName));
    HBaseTestUtils.util.truncateTable(Bytes.toBytes(compositeTableName));
    HBaseTestUtils.util.truncateTable(Bytes.toBytes(incrementTableName));
    HBaseTestUtils.util.truncateTable(Bytes.toBytes(managedTableName));
  }

  private PartitionKey createKey(PartitionStrategy partitionStrategy,
      long uniqueIdx) {
    return new PartitionKey("part1_" + uniqueIdx, "part2_" + uniqueIdx);
  }

  private GenericRecord createGenericEntity(long uniqueIdx) {
    return createGenericEntity(uniqueIdx, testRecord);
  }

  /**
   * Creates a generic entity with the given schema, which must be migratable
   * from TestRecord. Data from TestRecord is filled in.
   * 
   * @param uniqueIdx
   * @param schemaString
   * @return
   */
  private GenericRecord createGenericEntity(long uniqueIdx, String schemaString) {
    String iStr = Long.toString(uniqueIdx);

    Schema.Parser parser = new Schema.Parser();
    Schema entitySchema = parser.parse(schemaString);
    Schema embeddedSchema = entitySchema.getField("field4").schema();
    Schema arrayValueSchema = entitySchema.getField("field5").schema()
        .getElementType();

    GenericRecord entity = new GenericData.Record(entitySchema);
    entity.put("keyPart1", "part1_" + iStr);
    entity.put("keyPart2", "part2_" + iStr);
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

  private TestRecord createSpecificEntity(long uniqueIdx) {
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

    TestRecord entity = TestRecord.newBuilder().setKeyPart1("part1_" + iStr)
        .setKeyPart2("part2_" + iStr).setField1("field1_" + iStr)
        .setField2("field2_" + iStr).setEnum$(TestEnum.ENUM3)
        .setField3(field3Map).setField4(embeddedRecord)
        .setField5(arrayRecordList).build();
    return entity;
  }

  @SuppressWarnings("unchecked")
  private void compareEntitiesWithString(long uniqueIdx, IndexedRecord record) {
    String iStr = Long.toString(uniqueIdx);
    assertEquals("part1_" + iStr, record.get(0));
    assertEquals("part2_" + iStr, record.get(1));
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

  @SuppressWarnings("unchecked")
  private void compareEntitiesWithUtf8(long uniqueIdx, IndexedRecord record) {
    String iStr = Long.toString(uniqueIdx);
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

  @Test
  public void testGeneric() throws Exception {
    Dao<GenericRecord> dao = new GenericAvroDao(tablePool, tableName,
        "TestRecord", manager, testRecord);

    // Create the new entities
    for (int i = 0; i < 10; ++i) {
      GenericRecord entity = createGenericEntity(i);
      dao.put(entity);
    }

    // ensure the new entities are what we expect with get operations
    for (int i = 0; i < 10; ++i) {
      compareEntitiesWithUtf8(i,
          dao.get(createKey(dao.getPartitionStrategy(), i)));
    }

    // ensure the new entities are what we expect with scan operations
    int cnt = 0;
    EntityScanner<GenericRecord> entityScanner = dao.getScanner();
    entityScanner.initialize();
    try {
      for (GenericRecord entity : entityScanner) {
        compareEntitiesWithUtf8(cnt, entity);
        cnt++;
      }
      assertEquals(10, cnt);
    } finally {
      entityScanner.close();
    }
  }

  @Test
  public void testSpecific() throws Exception {
    Dao<TestRecord> dao = new SpecificAvroDao<TestRecord>(tablePool, tableName,
        "TestRecord", manager);

    // Create the new entities
    for (int i = 0; i < 10; ++i) {
      TestRecord entity = createSpecificEntity(i);
      dao.put(entity);
    }

    // ensure the new entities are what we expect with get operations
    for (int i = 0; i < 10; ++i) {
      compareEntitiesWithString(i,
          dao.get(createKey(dao.getPartitionStrategy(), i)));
    }

    // ensure the new entities are what we expect with scan operations
    int cnt = 0;
    EntityScanner<TestRecord> entityScanner = dao.getScanner();
    entityScanner.initialize();
    try {
      for (TestRecord entity : entityScanner) {
        compareEntitiesWithString(cnt, entity);
        cnt++;
      }
      assertEquals(10, cnt);
    } finally {
      entityScanner.close();
    }
  }

  @Test
  public void testMigrateEntities() throws Exception {
    Dao<TestRecord> dao = new SpecificAvroDao<TestRecord>(tablePool, tableName,
        "TestRecord", manager);

    manager.migrateSchema(tableName, "TestRecord", goodMigrationRecordAddField);

    Dao<TestRecord> afterDao = new SpecificAvroDao<TestRecord>(tablePool,
        tableName, "TestRecord", manager);

    // Create the new entities
    for (int i = 0; i < 10; ++i) {
      TestRecord entity = createSpecificEntity(i);
      afterDao.put(entity);
    }

    // ensure the new entities are what we expect with get operations
    for (int i = 0; i < 10; ++i) {
      compareEntitiesWithString(i,
          dao.get(createKey(dao.getPartitionStrategy(), i)));
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMigrateAndPut() throws Exception {
    Dao<GenericRecord> dao = new GenericAvroDao(tablePool, tableName,
        "TestRecord", manager, testRecord);

    manager.migrateSchema(tableName, "TestRecord", goodMigrationRecordAddField);

    SchemaManager afterManager = new DefaultSchemaManager(tablePool);
    Dao<GenericRecord> afterDao = new GenericAvroDao(tablePool, tableName,
        "TestRecord", afterManager, goodMigrationRecordAddField);

    // Create the new entities
    for (int i = 0; i < 10; ++i) {
      GenericRecord entity = createGenericEntity(i, goodMigrationRecordAddField);
      entity.put("fieldToAdd1", i);
      entity.put("fieldToAdd2", i);
      for (GenericRecord rec : (List<GenericRecord>) entity.get("field5")) {
        rec.put("subfield4", String.valueOf(i));
      }
      afterDao.put(entity);
    }

    // ensure the new entities are what we expect with get operations
    for (int i = 0; i < 10; ++i) {
      // When getting with old dao, expect no values for fieldToAdd
      GenericRecord rec = dao.get(createKey(dao.getPartitionStrategy(), i));
      compareEntitiesWithUtf8(i, rec);
      assertEquals(null, rec.get("fieldToAdd1"));
      assertEquals(null, rec.get("fieldToAdd2"));

      rec = afterDao.get(createKey(dao.getPartitionStrategy(), i));
      compareEntitiesWithUtf8(i, rec);
      assertEquals(i, rec.get("fieldToAdd1"));
      assertEquals(i, rec.get("fieldToAdd2"));
      for (GenericRecord innerRec : (List<GenericRecord>) rec.get("field5")) {
        assertEquals(String.valueOf(i), innerRec.get("subfield4").toString());
      }
    }

    // ensure the new entities are what we expect with scan operations
    int cnt = 0;
    EntityScanner<GenericRecord> entityScanner = dao.getScanner();
    entityScanner.initialize();
    try {
      for (GenericRecord entity : entityScanner) {
        compareEntitiesWithUtf8(cnt, entity);
        cnt++;
      }
      assertEquals(10, cnt);
    } finally {
      entityScanner.close();
    }
  }

  /**
   * GenericAvroDaos created without a entity schema (but with a manager) should
   * use the newest available
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testDynamicGenericDao() throws Exception {
    Dao<GenericRecord> dao = new GenericAvroDao(tablePool, tableName,
        "TestRecord", manager);

    manager.migrateSchema(tableName, "TestRecord", goodMigrationRecordAddField);

    SchemaManager afterManager = new DefaultSchemaManager(tablePool);
    Dao<GenericRecord> afterDao = new GenericAvroDao(tablePool, tableName,
        "TestRecord", afterManager);

    // Create an entity with each dao.
    PartitionKey key1 = createKey(dao.getPartitionStrategy(), 1);
    GenericRecord entity1 = createGenericEntity(1, testRecordv2);
    for (GenericRecord rec : (List<GenericRecord>) entity1.get("field5")) {
      rec.put("subfield4", new Utf8(String.valueOf(2)));
    }
    dao.put(entity1);
    PartitionKey key2 = createKey(dao.getPartitionStrategy(), 2);
    GenericRecord entity2 = createGenericEntity(2, goodMigrationRecordAddField);
    entity2.put("fieldToAdd1", 2);
    entity2.put("fieldToAdd2", 2);
    for (GenericRecord rec : (List<GenericRecord>) entity2.get("field5")) {
      rec.put("subfield4", new Utf8(String.valueOf(2)));
    }
    afterDao.put(entity2);

    // ensure the new entities are what we expect with get operations
    GenericRecord entity1After = dao.get(key1);
    compareEntitiesWithUtf8(1, entity1After);
    assertNull(entity1After.get("fieldToAdd1"));
    assertNull(entity1After.get("fieldToAdd2"));
    GenericRecord entity2After = dao.get(key2);
    compareEntitiesWithUtf8(2, entity2After);
    assertNull(entity2After.get("fieldToAdd1"));
    assertNull(entity2After.get("fieldToAdd2"));

    entity1After = afterDao.get(key1);
    compareEntitiesWithUtf8(1, entity1After);
    // These should have gotten default values this time.
    assertNotNull(entity1After.get("fieldToAdd1"));
    assertNotNull(entity1After.get("fieldToAdd2"));
    entity2After = afterDao.get(key2);
    compareEntitiesWithUtf8(2, entity2After);
    assertEquals(2, entity2After.get("fieldToAdd1"));
    assertEquals(2, entity2After.get("fieldToAdd2"));
  }

  @Test
  public void testIncrement() {
    Dao<TestIncrement> dao = new SpecificAvroDao<TestIncrement>(tablePool,
        incrementTableName, "TestIncrement", manager);

    TestIncrement entity = TestIncrement.newBuilder().setKeyPart1("part1")
        .setKeyPart2("part2").setField1(10).build();
    dao.put(entity);

    PartitionKey key = new PartitionKey("part1", "part2");
    dao.increment(key, "field1", 10);
    assertEquals(20L, (long) dao.get(key).getField1());

    dao.increment(key, "field1", 5);
    assertEquals(25L, (long) dao.get(key).getField1());
  }
  
  @Test
  public void testComposite() {
    // Construct Dao
    Dao<CompositeRecord> dao = SpecificAvroDao.buildCompositeDaoWithEntityManager(tablePool,
        compositeTableName, CompositeRecord.class, manager);
    Dao<SubRecord1> subRecord1Dao = new SpecificAvroDao<SubRecord1>(tablePool, compositeTableName, "SubRecord1", manager);
    Dao<SubRecord2> subRecord2Dao = new SpecificAvroDao<SubRecord2>(tablePool, compositeTableName, "SubRecord2", manager);

    // Construct records
    SubRecord1 subRecord1 = SubRecord1.newBuilder().setKeyPart1("1")
        .setKeyPart2("1").setField1("field1_1").setField2("field1_2").build();
    SubRecord2 subRecord2 = SubRecord2.newBuilder().setKeyPart1("1")
        .setKeyPart2("1").setField1("field2_1").setField2("field2_2").build();

    CompositeRecord compositeRecord = CompositeRecord.newBuilder()
        .setSubRecord1(subRecord1).setSubRecord2(subRecord2).build();

    // Test put
    assertTrue(dao.put(compositeRecord));
    
    // validate deleting one of the records doesn't delete the entire row
    PartitionKey key = new PartitionKey("1", "1");
    subRecord2Dao.delete(key);
    subRecord1 = subRecord1Dao.get(key);
    assertNotNull(subRecord1);
    assertNull(subRecord2Dao.get(key));
    
    // validate the _s columns (like OCCVersion fields) weren't messed with
    assertEquals(1L, (long)subRecord1.getVersion());
    
    // validate fetching as composite after a delete of one still works.
    compositeRecord = dao.get(key);
    assertNotNull(compositeRecord.getSubRecord1());
    assertNull(compositeRecord.getSubRecord2());
  }

  @Test(expected = IncompatibleSchemaException.class)
  public void testBadMigrationKeyField() throws Exception {
    badMigration(badMigrationRecordAddKeyField);
  }

  @Test(expected = IncompatibleSchemaException.class)
  public void testBadMigration1() throws Exception {
    badMigration(badMigrationRecordAddFieldNoDefault);
  }

  @Test(expected = IncompatibleSchemaException.class)
  public void testBadMigration2() throws Exception {
    badMigration(badMigrationRecordAddSubFieldNoDefault);
  }

  @Test(expected = IncompatibleSchemaException.class)
  public void testBadMigration3() throws Exception {
    badMigration(badMigrationRecordModifiedMapping);
  }

  @Test(expected = IncompatibleSchemaException.class)
  public void testBadMigrationDuplicateSchema() throws Exception {
    badMigration(testRecordv2);
  }

  @Test(expected = IncompatibleSchemaException.class)
  public void testBadMigrationIntToLong() throws Exception {
    SchemaManager manager = new DefaultSchemaManager(tablePool);
    manager.migrateSchema(tableName, "TestRecord", goodMigrationRecordAddField);
    manager.migrateSchema(tableName, "TestRecord", badMigrationRecordIntToLong);
  }

  @Test(expected = IncompatibleSchemaException.class)
  public void testBadCreateIncompatibleKey1() throws Exception {
    SchemaTool tool = new SchemaTool(new HBaseAdmin(HBaseTestUtils.getConf()),
        new DefaultSchemaManager(tablePool));
    tool.createOrMigrateSchema(tableName, badCreateIncompatibleKey1, false);
  }

  @Test(expected = IncompatibleSchemaException.class)
  public void testBadCreateIncompatibleKey2() throws Exception {
    SchemaTool tool = new SchemaTool(new HBaseAdmin(HBaseTestUtils.getConf()),
        new DefaultSchemaManager(tablePool));
    tool.createOrMigrateSchema(tableName, badCreateIncompatibleKey2, false);
  }

  @Test(expected = IncompatibleSchemaException.class)
  public void testBadCreateIncompatibleColumn1() throws Exception {
    SchemaTool tool = new SchemaTool(new HBaseAdmin(HBaseTestUtils.getConf()),
        new DefaultSchemaManager(tablePool));
    tool.createOrMigrateSchema(tableName, badCreateIncompatibleColumn1, false);
  }

  @Test(expected = IncompatibleSchemaException.class)
  public void testBadCreateIncompatibleColumn2() throws Exception {
    SchemaTool tool = new SchemaTool(new HBaseAdmin(HBaseTestUtils.getConf()),
        new DefaultSchemaManager(tablePool));
    tool.createOrMigrateSchema(tableName, badCreateIncompatibleColumn2, false);
  }

  @Test(expected = IncompatibleSchemaException.class)
  public void testBadCreateIncompatibleColumn3() throws Exception {
    SchemaTool tool = new SchemaTool(new HBaseAdmin(HBaseTestUtils.getConf()),
        new DefaultSchemaManager(tablePool));
    tool.createOrMigrateSchema(tableName, badCreateIncompatibleColumn3, false);
  }

  @Test
  public void testGoodMigrations() throws Exception {
    manager.migrateSchema(tableName, "TestRecord", goodMigrationRecordAddField);
    manager.migrateSchema(tableName, "TestRecord",
        goodMigrationRecordRemoveField);
    manager.migrateSchema(tableName, "TestRecord",
        goodMigrationRecordAddSubField);
  }

  @Test
  public void testCreate() throws Exception {
    AvroKeyEntitySchemaParser parser = new AvroKeyEntitySchemaParser();
    // Clear out what was set up in before()
    Dao<ManagedSchema> managedDao = new SpecificAvroDao<ManagedSchema>(
        tablePool, "managed_schemas", managedRecordString, ManagedSchema.class);

    managedDao.delete(new PartitionKey(tableName, "test"));

    SchemaManager manager = new DefaultSchemaManager(tablePool);
    try {
      manager.getEntityVersion(tableName, "test",
          parser.parseEntitySchema(testRecord));
      fail();
    } catch (SchemaNotFoundException e) {
      // This is what we expect
    }
    manager.createSchema(tableName, "test", testRecord,
        "org.kitesdk.data.hbase.avro.AvroKeyEntitySchemaParser",
        "org.kitesdk.data.hbase.avro.AvroKeySerDe",
        "org.kitesdk.data.hbase.avro.AvroEntitySerDe");
    assertEquals(
        0,
        manager.getEntityVersion(tableName, "test",
            parser.parseEntitySchema(testRecord)));
  }

  @Test(expected = IncompatibleSchemaException.class)
  public void testCannotCreateExisting() throws Exception {
    manager.createSchema(tableName, "TestRecord", goodMigrationRecordAddField,
        "org.kitesdk.data.hbase.avro.AvroKeyEntitySchemaParser",
        "org.kitesdk.data.hbase.avro.AvroKeySerDe",
        "org.kitesdk.data.hbase.avro.AvroEntitySerDe");
  }

  private void badMigration(String badMigration) throws Exception {
    badMigration("TestRecord", badMigration);
  }

  private void badMigration(String name, String badMigration) throws Exception {
    manager.migrateSchema(tableName, name, badMigration);
  }
}
