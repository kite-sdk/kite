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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.spi.AbstractMetadataProvider;
import org.kitesdk.data.spi.Compatibility;
import org.kitesdk.data.spi.PartitionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class HiveAbstractMetadataProvider extends AbstractMetadataProvider implements
    PartitionListener {

  private static final Logger LOG = LoggerFactory
      .getLogger(HiveAbstractMetadataProvider.class);

  protected final Configuration conf;
  private MetaStoreUtil metastore;

  HiveAbstractMetadataProvider(Configuration conf) {
    Preconditions.checkNotNull(conf, "Configuration cannot be null");
    this.conf = conf;
  }

  protected MetaStoreUtil getMetaStoreUtil() {
    if (metastore == null) {
      metastore = new MetaStoreUtil(conf);
    }
    return metastore;
  }

  /**
   * Returns whether the table is a managed hive table.
   * @param name a Table name
   * @return true if the table is managed, false otherwise
   * @throws DatasetNotFoundException If the table does not exist in Hive
   */
  protected boolean isManaged(String namespace, String name) {
    return isManaged(getMetaStoreUtil().getTable(namespace, name));
  }

  /**
   * Returns whether the table is a managed hive table.
   * @param name a Table name
   * @return true if the table is managed, false otherwise
   * @throws DatasetNotFoundException If the table does not exist in Hive
   */
  protected boolean isExternal(String namespace, String name) {
    return isExternal(getMetaStoreUtil().getTable(namespace, name));
  }

  @Override
  public DatasetDescriptor update(String namespace, String name, DatasetDescriptor descriptor) {
    Compatibility.checkDatasetName(namespace, name);
    Compatibility.checkDescriptor(descriptor);

    if (!exists(namespace, name)) {
      throw new DatasetNotFoundException("Table not found: " + name);
    }

    Table table = getMetaStoreUtil().getTable(namespace, name);
    HiveUtils.updateTableSchema(table, descriptor);
    getMetaStoreUtil().alterTable(table);
    return descriptor;
  }

  @Override
  public boolean delete(String namespace, String name) {
    Compatibility.checkDatasetName(namespace, name);

    // TODO: when switching off of HCatalog, this may need to be moved
    if (!exists(namespace, name)) {
      return false;
    }
    getMetaStoreUtil().dropTable(namespace, name);
    return true;
  }

  @Override
  public boolean exists(String namespace, String name) {
    Compatibility.checkDatasetName(namespace, name);

    return getMetaStoreUtil().exists(namespace, name);
  }

  @Override
  public Collection<String> namespaces() {
    Collection<String> databases = getMetaStoreUtil().getAllDatabases();
    List<String> databasesWithDatasets = Lists.newArrayList();
    for (String db : databases) {
      if (isNamespace(db)) {
        databasesWithDatasets.add(db);
      }
    }
    return databasesWithDatasets;
  }

  @Override
  public Collection<String> datasets(String namespace) {
    Collection<String> tables = getMetaStoreUtil().getAllTables(namespace);
    List<String> readableTables = Lists.newArrayList();
    for (String name : tables) {
      if (isReadable(namespace, name)) {
        readableTables.add(name);
      }
    }
    return readableTables;
  }

  /**
   * Returns true if there is at least one table in the give database that can
   * be read.
   *
   * @param database a Hive database name
   * @return {@code true} if there is at least one readable table in database
   * @see {@link #isReadable(String, String)}
   */
  private boolean isNamespace(String database) {
    Collection<String> tables = getMetaStoreUtil().getAllTables(database);
    for (String name : tables) {
      if (isReadable(database, name)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns true if the given table exists and can be read by this library.
   *
   * @param namespace a Hive database name
   * @param name a table name
   * @return {@code true} if the table exists and is supported
   */
  private boolean isReadable(String namespace, String name) {
    Table table = getMetaStoreUtil().getTable(namespace, name);
    if (isManaged(table) || isExternal(table)) { // readable table types
      try {
        // get a descriptor for the table. if this succeeds, it is readable
        HiveUtils.descriptorForTable(conf, table);
        return true;
      } catch (DatasetException e) {
        // not a readable table
      } catch (IllegalStateException e) {
        // not a readable table
      } catch (IllegalArgumentException e) {
        // not a readable table
      } catch (UnsupportedOperationException e) {
        // not a readable table
      }
    }
    return false;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void partitionAdded(String namespace, String name, String path) {
    getMetaStoreUtil().addPartition(namespace, name, path);
  }

  private boolean isManaged(Table table) {
    return TableType.MANAGED_TABLE.toString().equals(table.getTableType());
  }

  private boolean isExternal(Table table) {
    return TableType.EXTERNAL_TABLE.toString().equals(table.getTableType());
  }

}
