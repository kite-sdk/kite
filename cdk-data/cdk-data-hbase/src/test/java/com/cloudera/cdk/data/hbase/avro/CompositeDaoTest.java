// (c) Copyright 2011-2012 Cloudera, Inc.
package com.cloudera.cdk.data.hbase.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.cdk.data.hbase.Dao;
import com.cloudera.cdk.data.hbase.avro.entities.CompositeRecord;
import com.cloudera.cdk.data.hbase.avro.entities.SubRecord1;
import com.cloudera.cdk.data.hbase.avro.entities.SubRecord2;
import com.cloudera.cdk.data.hbase.avro.entities.TestKey;
import com.cloudera.cdk.data.hbase.testing.HBaseTestUtils;

public class CompositeDaoTest {

  private static final String keyString;
  private static final String subRecord1String;
  private static final String subRecord2String;
  private static final String tableName = "testtable";
  private HTablePool tablePool;

  static {
    try {
      keyString = AvroUtils.inputStreamToString(AvroDaoTest.class
          .getResourceAsStream("/TestKey.avsc"));
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
    Dao<TestKey, CompositeRecord> dao = SpecificAvroDao.buildCompositeDao(
        null, tablePool, tableName, keyString,
        Arrays.asList(subRecord1String, subRecord2String), TestKey.class,
        CompositeRecord.class);

    // Construct records and keys
    TestKey testKey = TestKey.newBuilder().setPart1("1").setPart2("1").build();

    SubRecord1 subRecord1 = SubRecord1.newBuilder().setField1("field1_1").setField2("field1_2").build();
    SubRecord2 subRecord2 = SubRecord2.newBuilder().setField1("field2_1").setField2("field2_2").build();;

    CompositeRecord compositeRecord = CompositeRecord.newBuilder().setSubRecord1(subRecord1).setSubRecord2(subRecord2).build();

    // Test put and get
    dao.put(testKey, compositeRecord);
    CompositeRecord returnedCompositeRecord = dao.get(testKey);
    assertEquals("field1_1",
        returnedCompositeRecord.getSubRecord1().getField1());
    assertEquals("field1_2",
        returnedCompositeRecord.getSubRecord1().getField2());
    assertEquals("field2_1",
        returnedCompositeRecord.getSubRecord2().getField1());
    assertEquals("field2_2",
        returnedCompositeRecord.getSubRecord2().getField2());

    // Test OCC
    assertFalse(dao.put(testKey, compositeRecord));
    assertTrue(dao.put(testKey, returnedCompositeRecord));

    // Test null field
    testKey = TestKey.newBuilder().setPart1("1").setPart2("2").build();
    compositeRecord = CompositeRecord.newBuilder().setSubRecord1(subRecord1).build();
    dao.put(testKey, compositeRecord);
    compositeRecord = dao.get(testKey);
    assertEquals(null, compositeRecord.getSubRecord2());
  }
}
