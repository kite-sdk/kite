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

import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Key;
import org.kitesdk.data.RandomAccessDataset;
import org.kitesdk.data.hbase.HBaseDatasetRepository;
import org.kitesdk.data.hbase.avro.entities.SubEntity1;
import org.kitesdk.data.hbase.avro.entities.SubEntity2;
import org.kitesdk.data.hbase.testing.HBaseTestUtils;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CompositeDatasetTest {

  private static final String subEntity1String;
  private static final String subEntity2String;
  private static final String tableName = "testtable";

  static {
    try {
      subEntity1String = AvroUtils.inputStreamToString(AvroDaoTest.class
          .getResourceAsStream("/SubEntity1.avsc"));
      subEntity2String = AvroUtils.inputStreamToString(AvroDaoTest.class
          .getResourceAsStream("/SubEntity2.avsc"));
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

  @Test
  public void testSpecific() throws Exception {

    HBaseDatasetRepository repo = new HBaseDatasetRepository.Builder()
        .configuration(HBaseTestUtils.getConf()).build();

    // create constituent datasets
    repo.create("default", tableName + ".SubEntity1", new DatasetDescriptor.Builder()
        .schema(SubEntity1.SCHEMA$)
        .build());
    repo.create("default", tableName + ".SubEntity2", new DatasetDescriptor.Builder()
        .schema(SubEntity2.SCHEMA$)
        .build());

    // create composite dataset
    RandomAccessDataset<Map<String, SpecificRecord>> ds = repo.load(
        "default", tableName + ".SubEntity1.SubEntity2");

    // Construct entities
    SubEntity1 subEntity1 = SubEntity1.newBuilder().setPart1("1").setPart2("1")
        .setField1("field1_1").setField2("field1_2").build();
    SubEntity2 subEntity2 = SubEntity2.newBuilder().setPart1("1").setPart2("1")
        .setField1("field2_1").setField2("field2_2").build();

    Map<String, SpecificRecord> compositeEntity = new HashMap<String, SpecificRecord>();
    compositeEntity.put("SubEntity1", subEntity1);
    compositeEntity.put("SubEntity2", subEntity2);

    // Test put and get
    ds.put(compositeEntity);

    Key key = new Key.Builder(ds).add("part1", "1").add("part2", "1").build();
    Map<String, SpecificRecord> returnedCompositeEntity = ds.get(key);
    assertNotNull("found entity", returnedCompositeEntity);
    assertEquals("field1_1", ((SubEntity1)returnedCompositeEntity.get("SubEntity1")).getField1());
    assertEquals("field1_2", ((SubEntity1)returnedCompositeEntity.get("SubEntity1")).getField2());
    assertEquals("field2_1", ((SubEntity2)returnedCompositeEntity.get("SubEntity2")).getField1());
    assertEquals("field2_2", ((SubEntity2)returnedCompositeEntity.get("SubEntity2")).getField2());

    // Test OCC
    assertFalse(ds.put(compositeEntity));
    assertTrue(ds.put(returnedCompositeEntity));

    // Test null field
    subEntity1 = SubEntity1.newBuilder(subEntity1).setPart2("2").build(); // different key
    compositeEntity = new HashMap<String, SpecificRecord>();
    compositeEntity.put("SubEntity1", subEntity1);
    ds.put(compositeEntity);
    returnedCompositeEntity = ds.get(new Key.Builder(ds).add("part1", "1").add("part2", "2").build());
    assertNull(returnedCompositeEntity.get("SubEntity2"));
  }
}
