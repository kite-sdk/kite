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

import java.io.IOException;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.View;
import org.kitesdk.data.hbase.avro.AvroUtils;
import org.kitesdk.data.hbase.avro.entities.ArrayRecord;
import org.kitesdk.data.hbase.avro.entities.EmbeddedRecord;
import org.kitesdk.data.hbase.avro.entities.TestEntity;
import org.kitesdk.data.hbase.avro.entities.TestEnum;
import org.kitesdk.data.hbase.testing.HBaseTestUtils;

import org.kitesdk.data.spi.AbstractRefinableView;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DaoViewTest {

  private static final String[] NAMES = new String[] { "part1", "part2" };
  private static final String testEntity;
  private static final String tableName = "testtable";
  private static final String managedTableName = "managed_schemas";
  private HBaseDatasetRepository repo;
  private DaoDataset<TestEntity> ds;

  static {
    try {
      testEntity = AvroUtils
          .inputStreamToString(HBaseDatasetRepositoryTest.class
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
    repo = new HBaseDatasetRepository.Builder().configuration(
        HBaseTestUtils.getConf()).build();
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral(testEntity).build();
    ds = (DaoDataset<TestEntity>) repo.create(
        "default", tableName, descriptor, TestEntity.class);
  }

  @After
  public void after() throws Exception {
    repo.delete("default", tableName);
    HBaseTestUtils.util.truncateTable(Bytes.toBytes(tableName));
    HBaseTestUtils.util.truncateTable(Bytes.toBytes(managedTableName));
  }

  @Test
  public void testRange() {
    populateTestEntities(10);

    final AbstractRefinableView<TestEntity> range = new DaoView<TestEntity>(ds, TestEntity.class)
            .fromAfter(NAMES[0], "1").to(NAMES[0], "9")
            .fromAfter(NAMES[1], "1").to(NAMES[1], "9");

    // Test entity range checks
    // Note that these are strings, not ints, so lexicographic ordering is used
    Assert.assertTrue(range.includes(newTestEntity("5", "5")));
    Assert.assertTrue(range.includes(newTestEntity("5", "55")));
    Assert.assertTrue(range.includes(newTestEntity("9", "89")));
    Assert.assertTrue(range.includes(newTestEntity("9", "9")));
    Assert.assertFalse(range.includes(newTestEntity("1", "1")));
    Assert.assertFalse(range.includes(newTestEntity("1", "0")));
    Assert.assertFalse(range.includes(newTestEntity("1", "10")));
    Assert.assertFalse(range.includes(newTestEntity("9", "99")));

    DatasetReader<TestEntity> reader = range.newReader();
    int cnt = 2;
    try {
      for (TestEntity entity : reader) {
        Assert.assertEquals(Integer.toString(cnt), entity.getPart1());
        Assert.assertEquals(Integer.toString(cnt), entity.getPart2());
        cnt++;
      }
    } finally {
      reader.close();
    }

    Assert.assertEquals(10, cnt);
  }

  @Test
  public void testLimitedReader() {
    populateTestEntities(10);

    AbstractRefinableView<TestEntity> range = new DaoView<TestEntity>(ds, TestEntity.class)
        .from(NAMES[0], "0").to(NAMES[0], "9")
        .from(NAMES[1], "0").to(NAMES[1], "9");
    validRange(range, 0, 10);

    range = new DaoView<TestEntity>(ds, TestEntity.class)
        .fromAfter(NAMES[0], "1").to(NAMES[0], "9")
        .fromAfter(NAMES[1], "1").to(NAMES[1], "9");
    validRange(range, 2, 10);

    range = new DaoView<TestEntity>(ds, TestEntity.class)
        .from(NAMES[0], "0").toBefore(NAMES[0], "9")
        .from(NAMES[1], "0").toBefore(NAMES[1], "9");
    validRange(range, 0, 9);
  }

  @Test
  public void testLimitedWriter() {
    final View<TestEntity> range = ds
        .fromAfter(NAMES[0], "1").to(NAMES[0], "5")
        .fromAfter(NAMES[1], "1").to(NAMES[1], "5");
    DatasetWriter<TestEntity> writer = range.newWriter();
    try {
      writer.write(newTestEntity("3", "3"));
      writer.write(newTestEntity("5", "5"));
    } finally {
      writer.close();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidLimitedWriter() {
    final View<TestEntity> range = ds
        .fromAfter(NAMES[0], "1").to(NAMES[0], "5")
        .fromAfter(NAMES[1], "1").to(NAMES[1], "5");
    range.newWriter().write(newTestEntity("6", "6"));
  }

  @Test
  public void testEmptyCheck() throws IOException {
    DaoView<TestEntity> unbounded = new DaoView<TestEntity>(ds, TestEntity.class);

    Assert.assertTrue("New dataset should be empty", unbounded.isEmpty());

    populateTestEntities(1);

    Assert.assertFalse("Should not be empty after write", unbounded.isEmpty());

    Assert.assertFalse("Should find entity 0", unbounded
        .with(NAMES[0], "0").with(NAMES[1], "0")
        .isEmpty());

    Assert.assertTrue("Should not find entity 1", unbounded
        .with(NAMES[0], "1").with(NAMES[1], "1")
        .isEmpty());

  }

  private TestEntity newTestEntity(String part1, String part2) {
    return TestEntity
        .newBuilder()
        .setPart1(part1)
        .setPart2(part2)
        .setField1("field1")
        .setField2("field2")
        .setField3(new HashMap<String, String>())
        .setField4(
            EmbeddedRecord.newBuilder().setEmbeddedField1("embeddedField1")
                .setEmbeddedField2(2).build())
        .setField5(new ArrayList<ArrayRecord>()).setEnum$(TestEnum.ENUM1)
        .build();
  }

  private void populateTestEntities(int num) {
    for (int i = 0; i < num; i++) {
      ds.put(newTestEntity(Integer.toString(i), Integer.toString(i)));
    }
  }
  
  private void validRange(View<TestEntity> range, int startIdx, int endIdx) {
    int cnt = startIdx;
    DatasetReader<TestEntity> reader = range.newReader();
    try {
      for (TestEntity entity : reader) {
        Assert.assertEquals(Integer.toString(cnt), entity.getPart1());
        Assert.assertEquals(Integer.toString(cnt), entity.getPart2());
        cnt++;
      }
    } finally {
      reader.close();
    }
    Assert.assertEquals(endIdx, cnt);
  }
}
