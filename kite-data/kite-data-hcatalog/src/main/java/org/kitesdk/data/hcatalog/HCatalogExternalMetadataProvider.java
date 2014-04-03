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

import java.net.URI;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetExistsException;
import org.kitesdk.data.impl.Accessor;
import org.kitesdk.data.spi.filesystem.FileSystemUtil;
import org.kitesdk.data.DatasetIOException;
import com.google.common.base.Preconditions;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.kitesdk.data.spi.Compatibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HCatalogExternalMetadataProvider extends HCatalogMetadataProvider {

  private static final Logger logger = LoggerFactory
      .getLogger(HCatalogExternalMetadataProvider.class);
  private final Path rootDirectory;
  private final FileSystem rootFileSystem;

  public HCatalogExternalMetadataProvider(Configuration conf, Path rootDirectory) {
    super(conf);
    Preconditions.checkArgument(rootDirectory != null, "Root cannot be null");

    try {
      this.rootFileSystem = rootDirectory.getFileSystem(conf);
      this.rootDirectory = rootFileSystem.makeQualified(rootDirectory);
    } catch (IOException ex) {
      throw Accessor.getDefault().providerExceptionFor(
          new DatasetIOException("Could not get FileSystem for root path", ex));
    }
  }

  @Override
  public DatasetDescriptor load(String name) {
    Compatibility.checkDatasetName(name);

    final Table table = getHcat().getTable(HiveUtils.DEFAULT_DB, name);

    if (!TableType.EXTERNAL_TABLE.equals(table.getTableType())) {
      throw Accessor.getDefault().providerExceptionFor(
          new DatasetException("Table is not external"));
    }

    return HiveUtils.descriptorForTable(conf, table);
  }

  @Override
  public DatasetDescriptor create(String name, DatasetDescriptor descriptor) {
    Compatibility.checkDatasetName(name);
    Compatibility.checkDescriptor(descriptor);

    if (exists(name)) {
      throw new DatasetExistsException(
          "Metadata already exists for dataset:" + name);
    }

    logger.info("Creating an external Hive table named: " + name);

    // create a new descriptor with the dataset's location
    final DatasetDescriptor newDescriptor =
        new DatasetDescriptor.Builder(descriptor)
        .location(pathForDataset(name))
        .build();

    // create the data directory first so it is owned by the current user, not Hive
    FileSystemUtil.ensureLocationExists(newDescriptor, conf);

    // this object will be the table metadata
    final Table table = HiveUtils.tableForDescriptor(
        name, newDescriptor, true /* external table */ );

    // assign the location of the the table
    getHcat().createTable(table);

    return newDescriptor;
  }

  private Path pathForDataset(String name) {
    Preconditions.checkState(rootDirectory != null,
      "Dataset repository root directory can not be null");

    return rootFileSystem.makeQualified(
        HiveUtils.pathForDataset(rootDirectory, name));
  }
}
