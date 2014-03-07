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
package org.kitesdk.data.hbase.testing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;

import org.kitesdk.data.hbase.impl.SchemaManager;
import org.kitesdk.data.hbase.manager.DefaultSchemaManager;
import org.kitesdk.data.hbase.tool.SchemaTool;

public class HBaseTestUtils {

  public static final HBaseTestingUtility util = new HBaseTestingUtility();
  private static volatile MiniHBaseCluster miniCluster;
  private static volatile Configuration conf;
  private static final List<String> tablesSetup = new ArrayList<String>();

  public static MiniHBaseCluster getMiniCluster() throws Exception {
    if (miniCluster == null) {
      synchronized (HBaseTestUtils.class) {
        if (miniCluster == null) {
          getConf();
          miniCluster = util.startMiniCluster();
          util.createTable(Bytes.toBytes("epoch"),
              new byte[][] { Bytes.toBytes("e") });
          util.createTable(Bytes.toBytes("managed_schemas"),
              new byte[][] { Bytes.toBytes("meta"), Bytes.toBytes("schema"),
                  Bytes.toBytes("_s") });
        }
      }
    }
    return miniCluster;
  }

  public static Configuration getConf() {
    if (conf == null) {
      synchronized (HBaseTestUtils.class) {
        if (conf == null) {
          conf = util.getConfiguration();
          conf.set("hadoop.log.dir", conf.get("hadoop.tmp.dir"));
          // don't run the regionserver UI (bind errors)
          conf.set("hbase.master.info.port", "-1");
          conf.set("hbase.regionserver.info.port.auto", "true");
        }
      }
    }
    return conf;
  }

  /**
   * This method will instantiate HBase instance using HBaseTestingUtility,
   * start the instance, and create the given tables.
   * 
   * @param tableDefinitions
   *          a mapping of table names to a list of column family names
   * @return an already-started HbaseTestingUtility instance with created tables
   */
  public static HBaseTestingUtility setupHbase(
      Map<String, List<String>> tableDefinitions) throws Exception {

    getMiniCluster();

    /*
     * For every table definition, if the table has already been created,
     * truncate it. Otherwise, Create tables from the supplied table definitions
     */
    for (Map.Entry<String, List<String>> entry : tableDefinitions.entrySet()) {
      String tableName = entry.getKey();
      List<String> columnFamilyNames = entry.getValue();
      if (tablesSetup.contains(tableName)) {
        util.truncateTable(Bytes.toBytes(tableName));
      } else {
        int numColumnFamilies = columnFamilyNames.size();
        byte[][] columnFamilyNamesBytes = new byte[numColumnFamilies][];
        for (int i = 0; i < numColumnFamilies; i++) {
          columnFamilyNamesBytes[i] = Bytes.toBytes(columnFamilyNames.get(i));
        }
        util.createTable(Bytes.toBytes(tableName), columnFamilyNamesBytes);
        tablesSetup.add(tableName);
      }
    }

    return util;
  }

  public static HTablePool startHBaseAndGetPool() throws Exception {
    getMiniCluster();
    return new HTablePool(getConf(), 10);
  }

  public static SchemaManager initializeSchemaManager(
      HTablePool tablePool, String directory) throws Exception {
    SchemaManager entityManager = new DefaultSchemaManager(
        tablePool);
    SchemaTool schemaTool = new SchemaTool(new HBaseAdmin(getConf()),
        entityManager);
    schemaTool.createOrMigrateSchemaDirectory(directory, true);
    return entityManager;
  }

  public static void truncateTables(Collection<String> tableNames)
      throws IOException {
    for (String tableName : tableNames) {
      util.truncateTable(Bytes.toBytes(tableName));
    }
  }
}
