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

import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetExistsException;
import org.kitesdk.data.spi.Compatibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HiveManagedMetadataProvider extends HiveAbstractMetadataProvider {

  private static final Logger LOG = LoggerFactory
      .getLogger(HiveManagedMetadataProvider.class);

  public HiveManagedMetadataProvider(Configuration conf) {
    super(conf);
    LOG.info("Default FS: " + conf.get("fs.defaultFS"));
  }

  @Override
  public DatasetDescriptor load(String namespace, String name) {
    Compatibility.checkDatasetName(namespace, name);

    final Table table = getMetaStoreUtil().getTable(namespace, name);

    return HiveUtils.descriptorForTable(conf, table);
  }

  @Override
  public DatasetDescriptor create(String namespace, String name, DatasetDescriptor descriptor) {
    Compatibility.checkDatasetName(namespace, name);
    Compatibility.checkDescriptor(descriptor);

    if (exists(namespace, name)) {
      throw new DatasetExistsException(
          "Metadata already exists for dataset:" + name);
    }

    LOG.info("Creating a managed Hive table named: " + name);

    // construct the table metadata from a descriptor
    final Table table = HiveUtils.tableForDescriptor(
        namespace, name, descriptor, false /* managed table */);

    // create it
    getMetaStoreUtil().createTable(table);

    // load the created table to get the data location
    final Table newTable = getMetaStoreUtil().getTable(namespace, name);

    try {
      return new DatasetDescriptor.Builder(descriptor)
          .location(newTable.getSd().getLocation())
          .build();
    } catch (URISyntaxException e) {
      throw new DatasetException(e);
    }
  }
}
