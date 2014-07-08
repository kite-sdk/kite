/**
 * Copyright 2014 Cloudera Inc.
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
package org.kitesdk.data.hbase.tool;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.client.HBaseAdmin;
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
import org.kitesdk.data.hbase.avro.SpecificAvroDao;
import org.kitesdk.data.hbase.avro.entities.SimpleHBaseRecord;
import org.kitesdk.data.hbase.impl.Dao;
import org.kitesdk.data.hbase.impl.SchemaManager;
import org.kitesdk.data.hbase.manager.DefaultSchemaManager;
import org.kitesdk.data.hbase.testing.HBaseTestUtils;

/**
 * Test of using schema tool to migrate schemas in a directory.
 */
public class SchemaToolTest {
  private static final String tableName = "simple";
  private static final String managedTableName = "managed_schemas";
  private static File simpleSchemaFile;
  private static String simpleSchema;

  private HTablePool tablePool;
  private SchemaManager manager;
  private SchemaTool tool;

  @BeforeClass
  public static void beforeClass() throws Exception {
    simpleSchema = AvroUtils.inputStreamToString(AvroDaoTest.class
        .getResourceAsStream("/SchemaTool/SimpleHBaseRecord.avsc"));
    simpleSchemaFile = FileUtils.toFile(AvroDaoTest.class
        .getResource("/SchemaTool/SimpleHBaseRecord.avsc"));
    
    HBaseTestUtils.getMiniCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HBaseTestUtils.util.deleteTable(Bytes.toBytes(tableName));
  }
  
  @Before
  public void before() throws Exception {
    tablePool = new HTablePool(HBaseTestUtils.getConf(), 10);
    manager = new DefaultSchemaManager(tablePool);
    tool = new SchemaTool(new HBaseAdmin(HBaseTestUtils.getConf()),
        manager);
  }

  @After
  public void after() throws Exception {
    tablePool.close();
    HBaseTestUtils.util.truncateTable(Bytes.toBytes(tableName));
    HBaseTestUtils.util.truncateTable(Bytes.toBytes(managedTableName));
  }

  @Test
  public void testMigrateDirectory() throws Exception {
    tool.createOrMigrateSchemaDirectory("classpath:SchemaTool", true);

    Dao<SimpleHBaseRecord> dao = new SpecificAvroDao<SimpleHBaseRecord>(
        tablePool, tableName, "SimpleHBaseRecord", manager);
    testBasicOperations(dao);
  }

  @Test
  public void testMigrateFile() throws Exception {
    tool.createOrMigrateSchemaFile(tableName, simpleSchemaFile, true);

    Dao<SimpleHBaseRecord> dao = new SpecificAvroDao<SimpleHBaseRecord>(
        tablePool, tableName, "SimpleHBaseRecord", manager);
    testBasicOperations(dao);
  }

  @Test
  public void testMigrateSchema() throws Exception {
    tool.createOrMigrateSchema(tableName, simpleSchema, true);

    Dao<SimpleHBaseRecord> dao = new SpecificAvroDao<SimpleHBaseRecord>(
        tablePool, tableName, "SimpleHBaseRecord", manager);
    testBasicOperations(dao);
  }

  private void testBasicOperations(Dao<SimpleHBaseRecord> dao) {
    SimpleHBaseRecord r1 = SimpleHBaseRecord.newBuilder().setField1("Field 1")
        .setKeyPart1("KeyPart 1").build();
    dao.put(r1);

    PartitionKey key = new PartitionKey("KeyPart 1");
    SimpleHBaseRecord r2 = dao.get(key);
    assertEquals(r1.getField1().toString(), r2.getField1().toString());
    assertEquals(r1.getKeyPart1().toString(), r2.getKeyPart1().toString());
  }
}
