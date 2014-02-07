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
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.spi.AbstractMetadataProvider;
import org.kitesdk.data.spi.PartitionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class HCatalogMetadataProvider extends AbstractMetadataProvider implements
    PartitionListener {

  private static final Logger logger = LoggerFactory
      .getLogger(HCatalogMetadataProvider.class);

  protected final Configuration conf;
  private HCatalog hcat;

  HCatalogMetadataProvider(Configuration conf) {
    Preconditions.checkArgument(conf != null, "Configuration cannot be null");
    this.conf = conf;
  }

  protected HCatalog getHcat() {
    if (hcat == null) {
      hcat = new HCatalog(conf);
    }
    return hcat;
  }

  @Override
  public DatasetDescriptor update(String name, DatasetDescriptor descriptor) {
    Preconditions.checkArgument(name != null, "Name cannot be null");
    Preconditions.checkArgument(descriptor != null,
        "Descriptor cannot be null");

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
    Preconditions.checkArgument(name != null, "Name cannot be null");

    // TODO: when switching off of HCatalog, this may need to be moved
    if (!exists(name)) {
      return false;
    }
    getHcat().dropTable(HiveUtils.DEFAULT_DB, name);
    return true;
  }

  @Override
  public boolean exists(String name) {
    Preconditions.checkArgument(name != null, "Name cannot be null");

    return getHcat().exists(HiveUtils.DEFAULT_DB, name);
  }

  @Override
  public Collection<String> list() {
    return getHcat().getAllTables(HiveUtils.DEFAULT_DB);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void partitionAdded(String name, String path) {
    getHcat().addPartition(HiveUtils.DEFAULT_DB, name, path);
  }
}
