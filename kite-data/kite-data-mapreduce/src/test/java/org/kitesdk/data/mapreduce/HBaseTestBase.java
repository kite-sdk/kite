/*
 * Copyright 2014 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.mapreduce;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.hbase.HBaseDatasetRepository;
import org.kitesdk.data.hbase.avro.AvroUtils;
import org.kitesdk.data.hbase.testing.HBaseTestUtils;

public class HBaseTestBase {

  protected static final String testGenericEntity;

  static {
    try {
      testGenericEntity = AvroUtils.inputStreamToString(HBaseTestBase.class
          .getResourceAsStream("/TestGenericEntity.avsc"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected DatasetRepository repo;

  protected static final String tableName = "testtable";
  protected static final String managedTableName = "managed_schemas";


  @After
  public void after() throws Exception {
    HBaseTestUtils.util.truncateTable(Bytes.toBytes(tableName));
    HBaseTestUtils.util.truncateTable(Bytes.toBytes(managedTableName));
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HBaseTestUtils.util.deleteTable(Bytes.toBytes(tableName));
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    HBaseTestUtils.getMiniCluster();
    // managed table should be created by HBaseDatasetRepository
    HBaseTestUtils.util.deleteTable(Bytes.toBytes(managedTableName));
  }

  @Before
  public void setUp() throws Exception {
    this.repo = new HBaseDatasetRepository.Builder().configuration(HBaseTestUtils.getConf()).build();
  }

}