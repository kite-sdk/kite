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

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetExistsException;
import org.kitesdk.data.spi.Compatibility;
import org.kitesdk.data.spi.filesystem.FileSystemUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HiveManagedMetadataProvider extends HiveAbstractMetadataProvider {

  private static final Logger LOG = LoggerFactory
      .getLogger(HiveManagedMetadataProvider.class);

  public HiveManagedMetadataProvider(Configuration conf) {
    super(conf);
  }

  @Override
  public DatasetDescriptor create(String namespace, String name, DatasetDescriptor descriptor) {
    Compatibility.checkDatasetName(namespace, name);
    Compatibility.checkDescriptor(descriptor);

    URI location = descriptor.getLocation();
    String resolved = resolveNamespace(namespace, name, location);
    if (resolved != null) {
      if (resolved.equals(namespace)) {
        // the requested dataset already exists
        throw new DatasetExistsException(
            "Metadata already exists for dataset: " + namespace + "." + name);
      } else {
        DatasetDescriptor loaded = load(resolved, name);
        // replacing old default.name table
        LOG.warn("Creating table managed table {}.{}: replaces default.{}",
            new Object[]{namespace, name, name});
        // validate that the new metadata can read the existing data
        Compatibility.checkUpdate(loaded, descriptor);
        // if the table in the default namespace matches, then the location is
        // either null (and should be set to the existing) or matches. either
        // way, use the loaded location.
        location = loaded.getLocation();
      }
    }

    LOG.info("Creating a managed Hive table named: " + name);

    boolean isExternal = (location != null);

    DatasetDescriptor toCreate = descriptor;
    if (isExternal) {
      // add the location to the descriptor that will be used
      toCreate = new DatasetDescriptor.Builder(descriptor)
          .location(location)
          .build();
    }

    // construct the table metadata from a descriptor
    Table table = HiveUtils.tableForDescriptor(
        namespace, name, toCreate, isExternal);

    // create it
    getMetaStoreUtil().createTable(table);

    // load the created table to get the final data location
    Table newTable = getMetaStoreUtil().getTable(namespace, name);

    DatasetDescriptor newDescriptor = new DatasetDescriptor.Builder(descriptor)
        .location(newTable.getSd().getLocation())
        .build();

    if (isExternal) {
      FileSystemUtil.ensureLocationExists(newDescriptor, conf);
    }

    return newDescriptor;
  }

  @Override
  protected URI expectedLocation(String namespace, String name) {
    return null;
  }
}
