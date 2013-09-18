package com.cloudera.cdk.data.hbase.avro;

import com.cloudera.cdk.data.Dataset;
import com.cloudera.cdk.data.DatasetAccessor;
import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.DatasetReader;
import com.cloudera.cdk.data.PartitionKey;
import com.cloudera.cdk.data.PartitionStrategy;
import com.cloudera.cdk.data.hbase.HBaseDatasetRepository;
import com.cloudera.cdk.data.hbase.avro.entities.ArrayRecord;
import com.cloudera.cdk.data.hbase.avro.entities.EmbeddedRecord;
import com.cloudera.cdk.data.hbase.avro.entities.TestEntity;
import com.cloudera.cdk.data.hbase.avro.entities.TestEnum;
import com.cloudera.cdk.data.hbase.avro.impl.AvroUtils;
import com.cloudera.cdk.data.hbase.manager.DefaultSchemaManager;
import com.cloudera.cdk.data.hbase.testing.HBaseTestUtils;
import com.cloudera.cdk.data.hbase.tool.SchemaTool;
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

public class HBaseDatasetRepositoryTest {

  private static final String testEntity;
  private static final String testGenericEntity;
  private static final String tableName = "testtable";
  private static final String managedTableName = "managed_schemas";

  private HTablePool tablePool;

  static {
    try {
      testEntity = AvroUtils.inputStreamToString(HBaseDatasetRepositoryTest.class
          .getResourceAsStream("/TestEntity.avsc"));
      testGenericEntity = AvroUtils.inputStreamToString(HBaseDatasetRepositoryTest.class
          .getResourceAsStream("/TestGenericEntity.avsc"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    HBaseTestUtils.getMiniCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HBaseTestUtils.util.deleteTable(Bytes.toBytes(tableName));
  }

  @Before
  public void before() throws Exception {
    tablePool = new HTablePool(HBaseTestUtils.getConf(), 10);
  }

  @After
  public void after() throws Exception {
    tablePool.close();
    HBaseTestUtils.util.truncateTable(Bytes.toBytes(tableName));
    HBaseTestUtils.util.truncateTable(Bytes.toBytes(managedTableName));
  }

  @Test
  public void testGeneric() throws Exception {

    HBaseAdmin hBaseAdmin = new HBaseAdmin(HBaseTestUtils.getConf());
    HBaseDatasetRepository repo = new HBaseDatasetRepository(hBaseAdmin, tablePool);

    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder()
        .identity("part1", 1).identity("part2", 2).get();

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(testGenericEntity)
        .partitionStrategy(partitionStrategy)
        .get();
    Dataset ds = repo.create(tableName, descriptor);
    DatasetAccessor<GenericRecord> accessor = ds.newAccessor();

    // Create the new entities
    for (int i = 0; i < 10; ++i) {
      GenericRecord entity = createGenericEntity(i);
      accessor.put(entity);
    }

    // reload
    ds = repo.load(tableName);
    accessor = ds.newAccessor();

    // ensure the new entities are what we expect with get operations
    for (int i = 0; i < 10; ++i) {
      String iStr = Long.toString(i);
      PartitionKey key = partitionStrategy.partitionKey("part1_" + iStr, "part2_" + iStr);
      compareEntitiesWithUtf8(i, accessor.get(key));
    }

    // ensure the new entities are what we expect with scan operations
    int cnt = 0;
    DatasetReader<GenericRecord> reader = ds.getReader();
    reader.open();
    try {
      for (GenericRecord entity : reader) {
        compareEntitiesWithUtf8(cnt, entity);
        cnt++;
      }
      assertEquals(10, cnt);
    } finally {
      reader.close();
    }
  }

  @Test
  public void testSpecific() throws Exception {
    HBaseAdmin hBaseAdmin = new HBaseAdmin(HBaseTestUtils.getConf());
    HBaseDatasetRepository repo = new HBaseDatasetRepository(hBaseAdmin, tablePool);

    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder()
        .identity("part1", 1).identity("part2", 2).get();

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(testEntity)
        .partitionStrategy(partitionStrategy)
        .get();
    Dataset ds = repo.create(tableName, descriptor);
    DatasetAccessor<TestEntity> accessor = ds.newAccessor();

    // Create the new entities
    for (int i = 0; i < 10; ++i) {
      TestEntity entity = createSpecificEntity(i);
      accessor.put(entity);
    }

    // ensure the new entities are what we expect with get operations
    for (int i = 0; i < 10; ++i) {
      String iStr = Long.toString(i);
      PartitionKey key = partitionStrategy.partitionKey("part1_" + iStr, "part2_" + iStr);
      compareEntitiesWithString(i, accessor.get(key));
    }

    // ensure the new entities are what we expect with scan operations
    int cnt = 0;
    DatasetReader<TestEntity> reader = ds.getReader();
    reader.open();
    try {
      for (TestEntity entity : reader) {
        compareEntitiesWithString(cnt, entity);
        cnt++;
      }
      assertEquals(10, cnt);
    } finally {
      reader.close();
    }
  }

  // TODO: remove duplication from ManagedDaoTest

  private GenericRecord createGenericEntity(long uniqueIdx) {
    return createGenericEntity(uniqueIdx, testGenericEntity);
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
  private void compareEntitiesWithUtf8(long uniqueIdx, IndexedRecord record) {
    String iStr = Long.toString(uniqueIdx);

    assertEquals(new String("part1_" + iStr), record.get(0).toString()); // TODO: check type
    assertEquals(new String("part2_" + iStr), record.get(1).toString()); // TODO: check type

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

    assertEquals(new String("part1_" + iStr), record.get(0).toString()); // TODO: check type
    assertEquals(new String("part2_" + iStr), record.get(1).toString()); // TODO: check type

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

    TestEntity entity = TestEntity.newBuilder()
        .setPart1("part1_" + iStr).setPart2("part2_" + iStr)
        .setField1("field1_" + iStr)
        .setField2("field2_" + iStr).setEnum$(TestEnum.ENUM3)
        .setField3(field3Map).setField4(embeddedRecord)
        .setField5(arrayRecordList).setIncrement(10L).build();
    return entity;
  }
}
