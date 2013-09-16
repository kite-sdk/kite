package com.cloudera.cdk.data.hbase.avro;

import com.cloudera.cdk.data.Dataset;
import com.cloudera.cdk.data.DatasetAccessor;
import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.DatasetReader;
import com.cloudera.cdk.data.PartitionKey;
import com.cloudera.cdk.data.PartitionStrategy;
import com.cloudera.cdk.data.hbase.HBaseDatasetRepository;
import com.cloudera.cdk.data.hbase.avro.entities.TestEnum;
import com.cloudera.cdk.data.hbase.avro.impl.AvroUtils;
import com.cloudera.cdk.data.hbase.testing.HBaseTestUtils;
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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HBaseDatasetRepositoryTest {

  private static final String testEntity;
  private static final String tableName = "testtable";
  static {
    try {
      testEntity = AvroUtils.inputStreamToString(HBaseDatasetRepositoryTest.class
          .getResourceAsStream("/TestEntity.avsc"));
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

  @Test
  public void testGeneric() throws Exception {

    HBaseAdmin hBaseAdmin = new HBaseAdmin(HBaseTestUtils.getConf());
    HTablePool tablePool = new HTablePool(HBaseTestUtils.getConf(), 10);
    HBaseDatasetRepository repo = new HBaseDatasetRepository(hBaseAdmin, tablePool);

    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder()
        .identity("part1", 1).identity("part2", 2).get();

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(testEntity)
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

  // TODO: remove duplication from ManagedDaoTest

  private GenericRecord createGenericEntity(long uniqueIdx) {
    return createGenericEntity(uniqueIdx, testEntity);
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
}
