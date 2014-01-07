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

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.FieldPartitioner;
import org.kitesdk.data.filesystem.impl.Accessor;
import org.kitesdk.data.spi.AbstractMetadataProvider;
import org.kitesdk.data.spi.PartitionListener;
import org.kitesdk.data.spi.StorageKey;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class HCatalogMetadataProvider extends AbstractMetadataProvider implements
    PartitionListener {

  private static final Logger logger = LoggerFactory
      .getLogger(HCatalogMetadataProvider.class);
  private static final Splitter PATH_SPLITTER = Splitter.on('/');

  protected final Configuration conf;
  final HCatalog hcat;

  HCatalogMetadataProvider(Configuration conf) {
    Preconditions.checkArgument(conf != null, "Configuration cannot be null");
    this.conf = conf;
    this.hcat = new HCatalog(conf);
  }

  @SuppressWarnings("deprecation")
  @Override
  public DatasetDescriptor update(String name, DatasetDescriptor descriptor) {
    Preconditions.checkArgument(name != null, "Name cannot be null");
    Preconditions.checkArgument(descriptor != null,
        "Descriptor cannot be null");

    if (!exists(name)) {
      throw new org.kitesdk.data.NoSuchDatasetException("Table not found: " + name);
    }

    Table table = hcat.getTable(HiveUtils.DEFAULT_DB, name);
    HiveUtils.updateTableSchema(table, descriptor);
    hcat.alterTable(table);
    return descriptor;
  }

  @Override
  public boolean delete(String name) {
    Preconditions.checkArgument(name != null, "Name cannot be null");

    // TODO: when switching off of HCatalog, this may need to be moved
    if (!exists(name)) {
      return false;
    }
    hcat.dropTable(HiveUtils.DEFAULT_DB, name);
    return true;
  }

  @Override
  public boolean exists(String name) {
    Preconditions.checkArgument(name != null, "Name cannot be null");

    return hcat.exists(HiveUtils.DEFAULT_DB, name);
  }

  @Override
  public Collection<String> list() {
    return hcat.getAllTables(HiveUtils.DEFAULT_DB);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void partitionAdded(String name, String key) {
    hcat.addPartition(HiveUtils.DEFAULT_DB, name,
        Lists.newArrayList(PATH_SPLITTER.split(key)));
  }
}
