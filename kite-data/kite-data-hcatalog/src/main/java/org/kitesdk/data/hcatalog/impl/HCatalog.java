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
package org.kitesdk.data.hcatalog.impl;

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hcatalog.common.HCatUtil;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.MetadataProviderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(HCatalog.class);

  private HiveMetaStoreClient client;
  private HiveConf hiveConf;

  public HCatalog(Configuration conf) {
    if (conf.get(Loader.HIVE_METASTORE_URI_PROP) == null) {
      LOG.warn("Using a local Hive MetaStore (for testing only)");
    }
    try {
      hiveConf = new HiveConf(conf, HiveConf.class);
      client = HCatUtil.getHiveClient(hiveConf);
    } catch (Exception e) {
      throw new RuntimeException("Hive metastore exception", e);
    }
  }

  public Table getTable(String dbName, String tableName) {
    Table table;
    try {
      table = HCatUtil.getTable(client, dbName, tableName);
    } catch (Exception e) {
      throw new DatasetNotFoundException("Hive table lookup exception", e);
    }
    
    if (table == null) {
      throw new DatasetNotFoundException("Could not find info for table: " + tableName);
    }
    return table;
  }
  
  public boolean tableExists(String dbName, String tableName) {
    try {
      return client.tableExists(dbName, tableName);
    } catch (Exception e) {
      throw new RuntimeException("Hive metastore exception", e);
    }
  }
  
  public void createTable(Table tbl) {
    try {
      client.createTable(tbl.getTTable());
    } catch (Exception e) {
      throw new RuntimeException("Hive table creation exception", e);
    }
  }

  public void alterTable(Table tbl) {
    try {
      client.alter_table(tbl.getDbName(), tbl.getTableName(), tbl.getTTable());
    } catch (Exception e) {
      throw new RuntimeException("Hive alter table exception", e);
    }
  }
  
  public void dropTable(String dbName, String tableName) {
    try {
      client.dropTable(dbName, tableName, true /* deleteData */,
          true /* ignoreUnknownTable */);
    } catch (Exception e) {
      throw new RuntimeException("Hive metastore exception", e);
    }
  }

  public void addPartition(String dbName, String tableName, String path) {
    try {
      // purposely don't check if the partition already exists because
      // getPartition(db, table, path) will throw an exception to indicate the
      // partition doesn't exist also. this way, it's only one call.
      client.appendPartition(dbName, tableName, path);
    } catch (AlreadyExistsException e) {
      // this is okay
    } catch (RuntimeException e) {
      throw new RuntimeException("Hive metastore exception", e);
    } catch (Exception e) {
      throw new RuntimeException("Hive metastore exception", e);
    }
  }

  public boolean exists(String dbName, String tableName) {
    try {
      return client.tableExists(dbName, tableName);
    } catch (Exception e) {
      throw new MetadataProviderException("Hive metastore exception", e);
    }
  }

  public List<String> getAllTables(String dbName) {
    try {
      return client.getAllTables(dbName);
    } catch (Exception e) {
      throw new MetadataProviderException("Hive metastore exception", e);
    }
  }

  public String getConf(String name, String def) throws Exception {
    return hiveConf.get(name, def);
  }
}
