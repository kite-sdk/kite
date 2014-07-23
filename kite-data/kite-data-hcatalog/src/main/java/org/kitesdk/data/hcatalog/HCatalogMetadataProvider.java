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

package org.kitesdk.data.hcatalog;

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
import org.kitesdk.data.UnknownFormatException;
import org.kitesdk.data.hcatalog.impl.HCatalog;
import org.kitesdk.data.spi.AbstractMetadataProvider;
import org.kitesdk.data.spi.Compatibility;
import org.kitesdk.data.spi.PartitionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class HCatalogMetadataProvider extends AbstractMetadataProvider implements
    PartitionListener {

  private static final Logger LOG = LoggerFactory
      .getLogger(HCatalogMetadataProvider.class);

  protected final Configuration conf;
  private HCatalog hcat;

  HCatalogMetadataProvider(Configuration conf) {
    Preconditions.checkNotNull(conf, "Configuration cannot be null");
    this.conf = conf;
  }

  protected HCatalog getHcat() {
    if (hcat == null) {
      hcat = new HCatalog(conf);
    }
    return hcat;
  }

  /**
   * Returns whether the table is a managed hive table.
   * @param name a Table name
   * @return true if the table is managed, false otherwise
   * @throws DatasetNotFoundException If the table does not exist in Hive
   */
  protected boolean isManaged(String name) {
    return isManaged(getHcat().getTable(HiveUtils.DEFAULT_DB, name));
  }

  /**
   * Returns whether the table is a managed hive table.
   * @param name a Table name
   * @return true if the table is managed, false otherwise
   * @throws DatasetNotFoundException If the table does not exist in Hive
   */
  protected boolean isExternal(String name) {
    return isExternal(getHcat().getTable(HiveUtils.DEFAULT_DB, name));
  }

  @Override
  public DatasetDescriptor update(String name, DatasetDescriptor descriptor) {
    Compatibility.checkDatasetName(name);
    Compatibility.checkDescriptor(descriptor);

    if (!exists(name)) {
      throw new DatasetNotFoundException("Table not found: " + name);
    }

    Table table = getHcat().getTable(HiveUtils.DEFAULT_DB, name);
    HiveUtils.updateTableSchema(table, descriptor);
    getHcat().alterTable(table);
    return descriptor;
  }

  @Override
  public boolean delete(String name) {
    Compatibility.checkDatasetName(name);

    // TODO: when switching off of HCatalog, this may need to be moved
    if (!exists(name)) {
      return false;
    }
    getHcat().dropTable(HiveUtils.DEFAULT_DB, name);
    return true;
  }

  @Override
  public boolean exists(String name) {
    Compatibility.checkDatasetName(name);

    return getHcat().exists(HiveUtils.DEFAULT_DB, name);
  }

  @Override
  public Collection<String> list() {
    Collection<String> tables = getHcat().getAllTables(HiveUtils.DEFAULT_DB);
    List<String> readableTables = Lists.newArrayList();
    for (String name : tables) {
      Table table = getHcat().getTable(HiveUtils.DEFAULT_DB, name);
      if (isManaged(table) || isExternal(table)) { // readable table types
        try {
          // get a descriptor for the table. if this succeeds, it is readable
          HiveUtils.descriptorForTable(conf, table);
          readableTables.add(name);
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
    }
    return readableTables;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void partitionAdded(String name, String path) {
    getHcat().addPartition(HiveUtils.DEFAULT_DB, name, path);
  }

  private boolean isManaged(Table table) {
    return TableType.MANAGED_TABLE.toString().equals(table.getTableType());
  }

  private boolean isExternal(Table table) {
    return TableType.EXTERNAL_TABLE.toString().equals(table.getTableType());
  }

}
