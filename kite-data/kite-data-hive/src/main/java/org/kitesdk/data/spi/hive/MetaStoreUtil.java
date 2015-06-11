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
package org.kitesdk.data.spi.hive;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.thrift.TException;
import org.kitesdk.data.DatasetExistsException;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.DatasetOperationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaStoreUtil {

  private static final Logger LOG = LoggerFactory.getLogger(MetaStoreUtil.class);
  private static final String ALLOW_LOCAL_METASTORE = "kite.hive.allow-local-metastore";

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

  public MetaStoreUtil(Configuration conf) {
    this.hiveConf = new HiveConf(conf, HiveConf.class);
    // Add the passed configuration back into the HiveConf to work around
    // a Hive bug that resets to defaults
    HiveUtils.addResource(hiveConf, conf);
    if (!allowLocalMetaStore(hiveConf) &&
        isEmpty(hiveConf, Loader.HIVE_METASTORE_URI_PROP)) {
      LOG.warn("Aborting use of local MetaStore. " +
          "Allow local MetaStore by setting {}=true in HiveConf",
          ALLOW_LOCAL_METASTORE);
      throw new IllegalArgumentException(
          "Missing Hive MetaStore connection URI");
    }
    try {
      client = new HiveMetaStoreClient(hiveConf);
    } catch (TException e) {
      throw new DatasetOperationException("Hive metastore exception", e);
    }
  }

  private boolean allowLocalMetaStore(HiveConf conf) {
    return conf.getBoolean(ALLOW_LOCAL_METASTORE, false);
  }

  private boolean isEmpty(HiveConf conf, String prop) {
    String value = conf.get(prop);
    return (value == null || value.isEmpty());
  }

  public Table getTable(final String dbName, final String tableName) {
    ClientAction<Table> getTable =
        new ClientAction<Table>() {
          @Override
          public Table call() throws TException {
            return new Table(client.getTable(dbName, tableName));
          }
        };

    Table table;
    try {
      table = doWithRetry(getTable);
    } catch (NoSuchObjectException e) {
      throw new DatasetNotFoundException(
          "Hive table not found: " + dbName + "." + tableName);
    } catch (MetaException e) {
      throw new DatasetNotFoundException("Hive table lookup exception", e);
    } catch (TException e) {
      throw new DatasetOperationException(
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
      throw new DatasetOperationException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new DatasetOperationException(
          "Exception communicating with the Hive MetaStore", e);
    }
  }

  public void createDatabase(final String dbName) {
    ClientAction<Void> create =
        new ClientAction<Void>() {
          @Override
          public Void call() throws TException {
            client.createDatabase(
                new Database(dbName, "Database created by Kite",
                    null /* default location */,
                    null /* no custom parameters */ ));
            return null;
          }
        };

    try {
      doWithRetry(create);
    } catch (AlreadyExistsException e) {
      throw new DatasetExistsException(
          "Hive database already exists: " + dbName, e);
    } catch (InvalidObjectException e) {
      throw new DatasetOperationException("Invalid database: " + dbName, e);
    } catch (MetaException e) {
      throw new DatasetOperationException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new DatasetOperationException(
          "Exception communicating with the Hive MetaStore", e);
    }
  }

  public void createTable(final Table tbl) {
    ClientAction<Void> create =
        new ClientAction<Void>() {
          @Override
          public Void call() throws TException {
            client.createTable(tbl);
            return null;
          }
        };

    try {
      createDatabase(tbl.getDbName());
    } catch (DatasetExistsException e) {
      // not a problem, use the existing database
    }

    try {
      doWithRetry(create);
    } catch (NoSuchObjectException e) {
      throw new DatasetNotFoundException("Hive table not found: " +
          tbl.getDbName() + "." + tbl.getTableName());
    } catch (AlreadyExistsException e) {
      throw new DatasetExistsException("Hive table already exists: " +
          tbl.getDbName() + "." + tbl.getTableName(), e);
    } catch (InvalidObjectException e) {
      throw new DatasetOperationException("Invalid table", e);
    } catch (MetaException e) {
      throw new DatasetOperationException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new DatasetOperationException(
          "Exception communicating with the Hive MetaStore", e);
    }
  }

  public void alterTable(final Table tbl) {
    ClientAction<Void> alter =
        new ClientAction<Void>() {
          @Override
          public Void call() throws TException {
            client.alter_table(
                tbl.getDbName(), tbl.getTableName(), tbl);
            return null;
          }
        };

    try {
      doWithRetry(alter);
    } catch (NoSuchObjectException e) {
      throw new DatasetNotFoundException("Hive table not found: " +
          tbl.getDbName() + "." + tbl.getTableName());
    } catch (InvalidObjectException e) {
      throw new DatasetOperationException("Invalid table", e);
    } catch (InvalidOperationException e) {
      throw new DatasetOperationException("Invalid table change", e);
    } catch (MetaException e) {
      throw new DatasetOperationException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new DatasetOperationException(
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
      throw new DatasetOperationException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new DatasetOperationException(
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
      throw new DatasetOperationException(
          "Invalid partition for " + dbName + "." + tableName + ": " + path, e);
    } catch (MetaException e) {
      throw new DatasetOperationException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new DatasetOperationException(
          "Exception communicating with the Hive MetaStore", e);
    }
  }

  public void dropPartition(final String dbName, final String tableName,
                            final String path) {
    ClientAction<Void> dropPartition =
        new ClientAction<Void>() {
          @Override
          public Void call() throws TException {

            client.dropPartition(dbName, tableName, path, false);
            return null;
          }
        };

    try {
      doWithRetry(dropPartition);
    } catch (NoSuchObjectException e) {
      // this is okay
    } catch (InvalidObjectException e) {
      throw new DatasetOperationException(
          "Invalid partition for " + dbName + "." + tableName + ": " + path, e);
    } catch (MetaException e) {
      throw new DatasetOperationException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new DatasetOperationException(
          "Exception communicating with the Hive MetaStore", e);
    }
  }

  public List<String> listPartitions(final String dbName,
                                     final String tableName,
                                     final short max) {
    ClientAction<List<String>> listPartitions =
        new ClientAction<List<String>>() {
          @Override
          public List<String> call() throws TException {
            List<Partition> partitions =
                client.listPartitions(dbName, tableName, max);
            List<String> paths = Lists.newArrayList();
            for (Partition partition : partitions) {
              paths.add(partition.getSd().getLocation());
            }
            return paths;
          }
        };
    try {
      return doWithRetry(listPartitions);
    } catch (NoSuchObjectException e) {
      return ImmutableList.of();
    } catch (MetaException e) {
      throw new DatasetOperationException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new DatasetOperationException(
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
      throw new DatasetOperationException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new DatasetOperationException(
          "Exception communicating with the Hive MetaStore", e);
    }
  }

  public List<String> getAllDatabases() {
    ClientAction<List<String>> create =
        new ClientAction<List<String>>() {
          @Override
          public List<String> call() throws TException {
            return client.getAllDatabases();
          }
        };

    try {
      return doWithRetry(create);
    } catch (NoSuchObjectException e) {
      return ImmutableList.of();
    } catch (MetaException e) {
      throw new DatasetOperationException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new DatasetOperationException(
          "Exception communicating with the Hive MetaStore", e);
    }
  }

  public void dropDatabase(final String name, final boolean deleteData) {
    ClientAction<Void> drop =
        new ClientAction<Void>() {

        @Override
        public Void call() throws TException {
          client.dropDatabase(name, deleteData, true);
          return null;
        }
      };

    try {
      doWithRetry(drop);
    } catch (NoSuchObjectException e) {
    } catch (MetaException e) {
      throw new DatasetOperationException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new DatasetOperationException(
          "Exception communicating with the Hive MetaStore", e);
    }
  }
}
