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

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.thrift.TException;
import org.kitesdk.data.DatasetExistsException;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.DatasetRepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(HCatalog.class);

  private final HiveMetaStoreClient client;
  private final HiveConf hiveConf;

  private static interface ClientAction<R> {
    R call() throws TException;
  }

  private <R> R doWithRetry(ClientAction<R> action) throws TException {
    try {
      synchronized (client) {
        return action.call();
      }
    } catch (TException e) {
      try {
        synchronized (client) {
          client.reconnect();
        }
      } catch (MetaException swallowedException) {
        // reconnect failed, throw the original exception
        throw e;
      }
      synchronized (client) {
        // retry the action. if this fails, its exception is propagated
        return action.call();
      }
    }
  }

  public HCatalog(Configuration conf) {
    if (conf.get(Loader.HIVE_METASTORE_URI_PROP) == null) {
      LOG.warn("Using a local Hive MetaStore (for testing only)");
    }
    try {
      hiveConf = new HiveConf(conf, HiveConf.class);
      client = HCatUtil.getHiveClient(hiveConf);
    } catch (TException e) {
      throw new DatasetRepositoryException("Hive metastore exception", e);
    } catch (IOException e) {
      throw new DatasetIOException("Hive metastore exception", e);
    }
  }

  public Table getTable(final String dbName, final String tableName) {
    ClientAction<Table> getTable =
        new ClientAction<Table>() {
          @Override
          public Table call() throws TException {
            return HCatUtil.getTable(client, dbName, tableName);
          }
        };

    Table table;
    try {
      table = doWithRetry(getTable);
    } catch (NoSuchObjectException e) {
      throw new DatasetNotFoundException("Hive table lookup exception", e);
    } catch (MetaException e) {
      throw new DatasetNotFoundException("Hive table lookup exception", e);
    } catch (TException e) {
      throw new DatasetRepositoryException(
          "Exception communicating with the Hive MetaStore", e);
    }

    if (table == null) {
      throw new DatasetNotFoundException("Could not find info for table: " + tableName);
    }
    return table;
  }
  
  public boolean tableExists(final String dbName, final String tableName) {
    ClientAction<Boolean> exists =
        new ClientAction<Boolean>() {
          @Override
          public Boolean call() throws TException {
            return client.tableExists(dbName, tableName);
          }
        };

    try {
      return doWithRetry(exists);
    } catch (UnknownDBException e) {
      return false;
    } catch (MetaException e) {
      throw new DatasetRepositoryException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new DatasetRepositoryException(
          "Exception communicating with the Hive MetaStore", e);
    }
  }
  
  public void createTable(final Table tbl) {
    ClientAction<Void> create =
        new ClientAction<Void>() {
          @Override
          public Void call() throws TException {
            client.createTable(tbl.getTTable());
            return null;
          }
        };

    try {
      doWithRetry(create);
    } catch (NoSuchObjectException e) {
      throw new DatasetNotFoundException("Hive table lookup exception", e);
    } catch (AlreadyExistsException e) {
      throw new DatasetExistsException("Hive table exists", e);
    } catch (InvalidObjectException e) {
      throw new DatasetRepositoryException("Invalid table", e);
    } catch (MetaException e) {
      throw new DatasetRepositoryException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new DatasetRepositoryException(
          "Exception communicating with the Hive MetaStore", e);
    }
  }

  public void alterTable(final Table tbl) {
    ClientAction<Void> alter =
        new ClientAction<Void>() {
          @Override
          public Void call() throws TException {
            client.alter_table(
                tbl.getDbName(), tbl.getTableName(), tbl.getTTable());
            return null;
          }
        };

    try {
      doWithRetry(alter);
    } catch (NoSuchObjectException e) {
      throw new DatasetNotFoundException("Hive table lookup exception", e);
    } catch (InvalidObjectException e) {
      throw new DatasetRepositoryException("Invalid table", e);
    } catch (InvalidOperationException e) {
      throw new DatasetRepositoryException("Invalid table change", e);
    } catch (MetaException e) {
      throw new DatasetRepositoryException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new DatasetRepositoryException(
          "Exception communicating with the Hive MetaStore", e);
    }
  }
  
  public void dropTable(final String dbName, final String tableName) {
    ClientAction<Void> drop =
        new ClientAction<Void>() {
          @Override
          public Void call() throws TException {
            client.dropTable(dbName, tableName, true /* deleteData */,
                true /* ignoreUnknownTable */);
            return null;
          }
        };

    try {
      doWithRetry(drop);
    } catch (NoSuchObjectException e) {
      // this is okay
    } catch (MetaException e) {
      throw new DatasetRepositoryException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new DatasetRepositoryException(
          "Exception communicating with the Hive MetaStore", e);
    }
  }

  public void addPartition(final String dbName, final String tableName,
                           final String path) {
    ClientAction<Void> addPartition =
        new ClientAction<Void>() {
          @Override
          public Void call() throws TException {
            // purposely don't check if the partition already exists because
            // getPartition(db, table, path) will throw an exception to indicate the
            // partition doesn't exist also. this way, it's only one call.
              client.appendPartition(dbName, tableName, path);
            return null;
          }
        };

    try {
      doWithRetry(addPartition);
    } catch (AlreadyExistsException e) {
      // this is okay
    } catch (InvalidObjectException e) {
      throw new DatasetRepositoryException("Invalid partition", e);
    } catch (MetaException e) {
      throw new DatasetRepositoryException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new DatasetRepositoryException(
          "Exception communicating with the Hive MetaStore", e);
    }
  }

  public boolean exists(String dbName, String tableName) {
    return tableExists(dbName, tableName);
  }

  public List<String> getAllTables(final String dbName) {
    ClientAction<List<String>> create =
        new ClientAction<List<String>>() {
          @Override
          public List<String> call() throws TException {
            return client.getAllTables(dbName);
          }
        };

    try {
      return doWithRetry(create);
    } catch (NoSuchObjectException e) {
      return ImmutableList.of();
    } catch (MetaException e) {
      throw new DatasetRepositoryException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new DatasetRepositoryException(
          "Exception communicating with the Hive MetaStore", e);
    }
  }
}
