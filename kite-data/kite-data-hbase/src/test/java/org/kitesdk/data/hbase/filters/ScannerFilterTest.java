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
package org.kitesdk.data.hbase.filters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

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

import org.kitesdk.data.spi.PartitionKey;
import org.kitesdk.data.hbase.avro.AvroDaoTest;
import org.kitesdk.data.hbase.avro.AvroUtils;
import org.kitesdk.data.hbase.avro.GenericAvroDao;
import org.kitesdk.data.hbase.impl.BaseDao;
import org.kitesdk.data.hbase.impl.Dao;
import org.kitesdk.data.hbase.impl.EntityScanner;
import org.kitesdk.data.hbase.impl.EntityScannerBuilder;
import org.kitesdk.data.hbase.testing.HBaseTestUtils;

public class ScannerFilterTest {

  private static final String recordString;
  private static final String tableName = "testtable";

  private HTablePool tablePool;

  static {
    try {
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

    Dao<GenericRecord> dao = new GenericAvroDao(tablePool, tableName,
        recordString);

    for (int i = 0; i < 100; ++i) {
      @SuppressWarnings("deprecation")
      GenericRecord entity = new GenericData.Record(Schema.parse(recordString));
      entity.put("keyPart1", "part1_" + i);
      entity.put("keyPart2", "part2_" + i);
      entity.put("field1", "field1_" + Integer.toString(i));
      entity.put("field2", "field2_" + Integer.toString(i));
      dao.put(entity);
    }

    // Add a few NULL values
    @SuppressWarnings("deprecation")
    GenericRecord entity = new GenericData.Record(Schema.parse(recordString));
    entity.put("keyPart1", "part1_NULL");
    entity.put("keyPart2", "part2_NULL");
    entity.put("field1", "");
    entity.put("field2", "");
    dao.put(entity);

    @SuppressWarnings("deprecation")
    GenericRecord entityMissing = new GenericData.Record(
        Schema.parse(recordString));
    entityMissing.put("keyPart1", "part1_MISSING");
    entityMissing.put("keyPart2", "part2_MISSING");
    entityMissing.put("field1", "field1_MISSING_FIELD2");
    dao.put(entityMissing);
  }

  @After
  public void afterTest() throws Exception {
    tablePool.close();
  }

  public void checkScannerYieldValues(
      EntityScanner<GenericRecord> entityScanner, Set<String> values) {
    // Scan and make sure all of the values are in the list
    int cnt = 0;

    entityScanner.initialize();
    try {
      for (GenericRecord entity : entityScanner) {
        assertTrue(values.contains(entity.get("field1").toString()));
        cnt++;
      }
      // Scanner should only yield rows equal to the amount of possible values
      assertEquals(cnt, values.size());
    } finally {
      entityScanner.close();
    }
  }

  @Test
  public void testPassAllEqualityFilter() throws Exception {
    BaseDao<GenericRecord> dao = new GenericAvroDao(tablePool, tableName,
        recordString);

    EntityScannerBuilder<GenericRecord> builder = dao.getScannerBuilder()
        .addEqualFilter("field1", "field1_2")
        .addEqualFilter("field2", "field2_2").setPassAllFilters(true);

    Set<String> possibleValues = new HashSet<String>();
    possibleValues.add("field1_2");

    checkScannerYieldValues(builder.build(), possibleValues);
  }

  @Test
  public void testPassOneEqualityFilter() throws Exception {
    BaseDao<GenericRecord> dao = new GenericAvroDao(tablePool, tableName,
        recordString);

    Set<String> possibleValues = new HashSet<String>();
    possibleValues.add("field1_1");
    possibleValues.add("field1_62");
    possibleValues.add("field1_54");
    possibleValues.add("field1_29");
    possibleValues.add("field1_18");

    EntityScannerBuilder<GenericRecord> builder = dao.getScannerBuilder();
    for (String possibleValue : possibleValues) {
      builder.addEqualFilter("field1", possibleValue);
    }

    builder.setPassAllFilters(false);
    checkScannerYieldValues(builder.build(), possibleValues);
  }

  @Test
  public void testPassOneRegexMatchFilter() throws Exception {
    BaseDao<GenericRecord> dao = new GenericAvroDao(tablePool, tableName,
        recordString);

    Set<String> possibleValues = new HashSet<String>();
    for (int i = 0; i < 10; i++) {
      possibleValues.add("field1_2" + Integer.toString(i));
    }

    for (int i = 0; i < 10; i++) {
      possibleValues.add("field1_5" + Integer.toString(i));
    }

    EntityScannerBuilder<GenericRecord> builder = dao.getScannerBuilder();
    builder.addFilter(new RegexEntityFilter(dao.getEntitySchema(), dao
        .getEntityMapper().getEntitySerDe(), "field1", "field1_2\\d"));
    builder.addFilter(new RegexEntityFilter(dao.getEntitySchema(), dao
        .getEntityMapper().getEntitySerDe(), "field1", "field1_5\\d"));
    builder.setPassAllFilters(false);
    checkScannerYieldValues(builder.build(), possibleValues);

  }

  @Test
  public void testPassAllRegexMatchFilter() throws Exception {
    BaseDao<GenericRecord> dao = new GenericAvroDao(tablePool, tableName,
        recordString);

    Set<String> possibleValues = new HashSet<String>();
    for (int i = 0; i < 10; i++) {
      possibleValues.add("field1_3" + Integer.toString(i));
    }

    EntityScannerBuilder<GenericRecord> builder = dao.getScannerBuilder();
    builder.addRegexMatchFilter("field1", "field1_3\\d");
    builder.addRegexMatchFilter("field2", "field2_3\\d");
    builder.setPassAllFilters(true);
    checkScannerYieldValues(builder.build(), possibleValues);
  }

  @Test
  public void testPassAllNotNullFilter() throws Exception {
    BaseDao<GenericRecord> dao = new GenericAvroDao(tablePool, tableName,
        recordString);

    Set<String> possibleValues = new HashSet<String>();
    for (int i = 0; i < 100; i++) {
      possibleValues.add("field1_" + Integer.toString(i));
    }
    possibleValues.add("field1_MISSING_FIELD2");

    EntityScannerBuilder<GenericRecord> builder = dao.getScannerBuilder();
    builder.addNotNullFilter("field1");
    checkScannerYieldValues(builder.build(), possibleValues);
  }

  @Test
  public void testPassIsNullFilter() throws Exception {
    BaseDao<GenericRecord> dao = new GenericAvroDao(tablePool, tableName,
        recordString);

    Set<String> possibleValues = new HashSet<String>();
    possibleValues.add("");

    EntityScannerBuilder<GenericRecord> builder = dao.getScannerBuilder();
    builder.addIsNullFilter("field1");
    checkScannerYieldValues(builder.build(), possibleValues);
  }

  @Test
  public void testPassIsMissingFilter() throws Exception {
    BaseDao<GenericRecord> dao = new GenericAvroDao(tablePool, tableName,
        recordString);

    Set<String> possibleValues = new HashSet<String>();
    possibleValues.add("field1_MISSING_FIELD2");

    EntityScannerBuilder<GenericRecord> builder = dao.getScannerBuilder();
    builder.addIsMissingFilter("field2");
    checkScannerYieldValues(builder.build(), possibleValues);
  }

  @Test
  public void testPassIfNotEqualFilter() throws Exception {
    BaseDao<GenericRecord> dao = new GenericAvroDao(tablePool, tableName,
        recordString);

    Set<String> possibleValues = new HashSet<String>();
    for (int i = 6; i < 100; i++) {
      possibleValues.add("field1_" + Integer.toString(i));
    }
    possibleValues.add("field1_MISSING_FIELD2");

    EntityScannerBuilder<GenericRecord> builder = dao.getScannerBuilder()
        .addNotEqualFilter("field1", "field1_0")
        .addNotEqualFilter("field1", "field1_1")
        .addNotEqualFilter("field1", "field1_2")
        .addNotEqualFilter("field1", "field1_3")
        .addNotEqualFilter("field1", "field1_4")
        .addNotEqualFilter("field1", "field1_5").addNotNullFilter("field1");

    checkScannerYieldValues(builder.build(), possibleValues);
  }

  @Test
  public void testStartRowScan() throws Exception {
    BaseDao<GenericRecord> dao = new GenericAvroDao(tablePool, tableName,
        recordString);

    Set<String> possibleValues = new HashSet<String>();
    possibleValues.add("field1_20");
    possibleValues.add("field1_21");
    possibleValues.add("field1_22");
    possibleValues.add("field1_23");
    possibleValues.add("field1_24");
    PartitionKey startKey = new PartitionKey("part1_20", "part2_20");
    PartitionKey stopKey = new PartitionKey("part1_25", "part2_25");

    EntityScannerBuilder<GenericRecord> builder = dao.getScannerBuilder()
        .setStartKey(startKey).setStopKey(stopKey);

    checkScannerYieldValues(builder.build(), possibleValues);
  }
}
