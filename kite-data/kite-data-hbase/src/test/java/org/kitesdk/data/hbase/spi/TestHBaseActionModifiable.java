/**
 * Copyright 2015 Cloudera Inc.
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

package org.kitesdk.data.hbase.spi;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.RandomAccessDataset;
import org.kitesdk.data.hbase.HBaseDatasetRepository;
import org.kitesdk.data.hbase.HBaseDatasetRepositoryTest;
import org.kitesdk.data.hbase.avro.AvroUtils;
import org.kitesdk.data.hbase.avro.entities.ArrayRecord;
import org.kitesdk.data.hbase.avro.entities.EmbeddedRecord;
import org.kitesdk.data.hbase.avro.entities.TestEntity;
import org.kitesdk.data.hbase.avro.entities.TestEnum;
import org.kitesdk.data.hbase.impl.PutAction;
import org.kitesdk.data.hbase.impl.PutActionModifier;
import org.kitesdk.data.hbase.impl.ScanModifier;
import org.kitesdk.data.hbase.testing.HBaseTestUtils;

import java.util.ArrayList;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for registering action modifiers
 */
public class TestHBaseActionModifiable {
  private static final String testEntity;
  private static final String tableName = "testtable";
  private static final String managedTableName = "managed_schemas";
  private HBaseDatasetRepository repo;
  private RandomAccessDataset<TestEntity> ds;

  static {
    try {
      testEntity = AvroUtils.inputStreamToString(
          HBaseDatasetRepositoryTest.class
              .getResourceAsStream("/TestEntity.avsc"));
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

  @Before
  public void setup() throws Exception {
    repo = new HBaseDatasetRepository.Builder()
        .configuration(HBaseTestUtils.getConf()).build();
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral(testEntity).build();
    ds = repo.create("default", tableName, descriptor, TestEntity.class);
  }

  @After
  public void after() throws Exception {
    ((HBaseActionModifiable) ds).clearAllModifiers();
    repo.delete("default", tableName);
    HBaseTestUtils.util.truncateTable(Bytes.toBytes(tableName));
    HBaseTestUtils.util.truncateTable(Bytes.toBytes(managedTableName));
  }

  @Test
  public void testGetActionModifiers() {
    HBaseActionModifiable amd = (HBaseActionModifiable) ds;
    PutActionModifier putActionModifier = newPutActionModifier("testModifierId",
        "TestValue");
    amd.registerPutActionModifier(putActionModifier);
    ScanModifier scanModifier = newScanModifier("OtherTestValue");
    amd.registerScanModifier(scanModifier);

    assertTrue(amd.getGetModifiers().isEmpty());
    assertTrue(amd.getDeleteActionModifiers().isEmpty());
    assertEquals(1, amd.getPutActionModifiers().size());
    assertEquals(putActionModifier, amd.getPutActionModifiers().get(0));
    assertEquals(1, amd.getScanModifiers().size());
    assertEquals(scanModifier, amd.getScanModifiers().get(0));
  }

  @Test
  public void testPutScanActionModifiers() {
    HBaseActionModifiable amd = (HBaseActionModifiable) ds;
    amd.registerPutActionModifier(
        newPutActionModifier("testModifierId", "TestValue"));
    saveTestEntity(newTestEntity());
    amd.registerScanModifier(newScanModifier("OtherTestValue"));
    checkRecord(false);

    amd.clearScanModifiers();
    amd.registerScanModifier(newScanModifier("TestValue"));
    checkRecord(true);
  }

  @Test
  public void testDuplicatePutActionModifiers() {
    HBaseActionModifiable amd = (HBaseActionModifiable) ds;
    // This should be replaced
    amd.registerPutActionModifier(
        newPutActionModifier("testModifiedId", "OtherTestValue"));
    amd.registerPutActionModifier(
        newPutActionModifier("testModifiedId", "TestValue"));

    saveTestEntity(newTestEntity());

    // Scan with OtherTestValue should not return any records
    amd.registerScanModifier(newScanModifier("OtherTestValue"));
    checkRecord(false);

    amd.clearScanModifiers();
    // Scan with TestValue should return records
    amd.registerScanModifier(newScanModifier("TestValue"));
    checkRecord(true);
  }

  private void checkRecord(boolean shouldExist) {
    DatasetReader<TestEntity> dsReader = ds.newReader();
    try {
      if (shouldExist) {
        assertTrue(dsReader.hasNext());
      } else {
        assertFalse(dsReader.hasNext());
      }
    } finally {
      dsReader.close();
    }
  }

  private ScanModifier newScanModifier(final String value) {
    return new ScanModifier() {
      @Override
      public Scan modifyScan(Scan scan) {
        scan.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("testcf"));
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
            Bytes.toBytes("meta"), Bytes.toBytes("testcf"),
            CompareFilter.CompareOp.EQUAL, Bytes.toBytes(value));
        filter.setFilterIfMissing(true);
        scan.setFilter(filter);
        return scan;
      }
    };
  }

  private PutActionModifier newPutActionModifier(String id, String value) {
    return new TestPutActionModifier(id, value);
  }

  private void saveTestEntity(TestEntity entity) {
    ds.put(entity);
  }

  private TestEntity newTestEntity() {
    return TestEntity.newBuilder().setPart1("part1").setPart2("part2")
        .setField1("field1").setField2("field2")
        .setField3(new HashMap<String, String>()).setField4(
            EmbeddedRecord.newBuilder().setEmbeddedField1("embeddedField1")
                .setEmbeddedField2(2).build())
        .setField5(new ArrayList<ArrayRecord>()).setEnum$(TestEnum.ENUM1)
        .build();
  }

  private class TestPutActionModifier implements PutActionModifier {
    private String id;
    private String value;

    public TestPutActionModifier(String id, String value) {
      this.id = id;
      this.value = value;
    }

    @Override
    public PutAction modifyPutAction(PutAction putAction) {
      Put put = putAction.getPut();
      put.add(Bytes.toBytes("meta"), Bytes.toBytes("testcf"),
          Bytes.toBytes(value));
      return new PutAction(put);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      TestPutActionModifier that = (TestPutActionModifier) o;
      return id.equals(that.id);
    }

    @Override
    public int hashCode() {
      return id.hashCode();
    }
  }

}
