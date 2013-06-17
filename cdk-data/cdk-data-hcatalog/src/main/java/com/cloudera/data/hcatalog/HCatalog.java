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
package com.cloudera.data.hcatalog;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hcatalog.common.HCatUtil;

final class HCatalog {

  private static HiveMetaStoreClient CLIENT_INSTANCE = null;
  
  private static synchronized HiveMetaStoreClient getClientInstance() {
    if (CLIENT_INSTANCE == null) {
      try {
        CLIENT_INSTANCE = HCatUtil.getHiveClient(new HiveConf());
      } catch (Exception e) {
        throw new RuntimeException("Could not connect to Hive", e);
      }
    }
    return CLIENT_INSTANCE;
  }
  
  public static Table getTable(String dbName, String tableName) {
    HiveMetaStoreClient client = getClientInstance();
    Table table;
    try {
      table = HCatUtil.getTable(client, dbName, tableName);
    } catch (Exception e) {
      throw new RuntimeException("Hive table lookup exception", e);
    }
    
    if (table == null) {
      throw new IllegalStateException("Could not find info for table: " + tableName);
    }
    return table;
  }
  
  public static boolean tableExists(String dbName, String tableName) {
    HiveMetaStoreClient client = getClientInstance();
    try {
      return client.tableExists(dbName, tableName);
    } catch (Exception e) {
      throw new RuntimeException("Hive metastore exception", e);
    }
  }
  
  public static void createTable(Table tbl) {
    HiveMetaStoreClient client = getClientInstance();
    try {
      client.createTable(tbl.getTTable());
    } catch (Exception e) {
      throw new RuntimeException("Hive table creation exception", e);
    }
  }
  
  public static void dropTable(String dbName, String tableName) {
    HiveMetaStoreClient client = getClientInstance();
    try {
      client.dropTable(dbName, tableName, true /* deleteData */,
          true /* ignoreUnknownTable */);
    } catch (Exception e) {
      throw new RuntimeException("Hive metastore exception", e);
    }
  }

  private HCatalog() { }
}
