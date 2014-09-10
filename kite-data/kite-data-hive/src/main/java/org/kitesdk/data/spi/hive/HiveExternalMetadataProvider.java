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

import com.google.common.base.Objects;
import java.net.URI;
import javax.annotation.Nullable;
import org.apache.hadoop.hive.metastore.api.Table;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetExistsException;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.spi.filesystem.FileSystemUtil;
import org.kitesdk.data.DatasetIOException;
import com.google.common.base.Preconditions;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.spi.Compatibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HiveExternalMetadataProvider extends HiveAbstractMetadataProvider {

  private static final String DEFAULT_NAMESPACE = "default";

  private static final Logger LOG = LoggerFactory
      .getLogger(HiveExternalMetadataProvider.class);
  private final Path rootDirectory;
  private final FileSystem rootFileSystem;

  public HiveExternalMetadataProvider(Configuration conf, Path rootDirectory) {
    super(conf);
    Preconditions.checkNotNull(rootDirectory, "Root cannot be null");

    try {
      this.rootFileSystem = rootDirectory.getFileSystem(conf);
      this.rootDirectory = rootFileSystem.makeQualified(rootDirectory);
    } catch (IOException ex) {
      throw new DatasetIOException("Could not get FileSystem for root path", ex);
    }
  }

  @Override
  public DatasetDescriptor load(String namespace, String name) {
    Compatibility.checkDatasetName(namespace, name);

    String resolved = resolveNamespace(namespace, name);
    if (resolved != null) {
      return HiveUtils.descriptorForTable(
          conf, getMetaStoreUtil().getTable(resolved, name));
    }
    throw new DatasetNotFoundException(
        "Hive table not found: " + namespace + "." + name);
  }

  @Override
  public DatasetDescriptor create(String namespace, String name, DatasetDescriptor descriptor) {
    Compatibility.checkDatasetName(namespace, name);
    Compatibility.checkDescriptor(descriptor);

    String resolved = resolveNamespace(
        namespace, name, descriptor.getLocation());
    if (resolved != null) {
      if (resolved.equals(namespace)) {
        // the requested dataset already exists
        throw new DatasetExistsException(
            "Metadata already exists for dataset: " + namespace + "." + name);
      } else {
        // replacing old default.name table
        LOG.warn("Creating table {}.{} for {}: replaces default.{}",
            new Object[]{
                namespace, name, pathForDataset(namespace, name), name});
        // validate that the new metadata can read the existing data
        Compatibility.checkUpdate(load(resolved, name), descriptor);
      }
    }

    LOG.info("Creating an external Hive table: {}.{}", namespace, name);

    DatasetDescriptor newDescriptor = descriptor;

    if (descriptor.getLocation() == null) {
      // create a new descriptor with the dataset's location
      newDescriptor = new DatasetDescriptor.Builder(descriptor)
          .location(pathForDataset(namespace, name))
          .build();
    }

    // create the data directory first so it is owned by the current user, not Hive
    FileSystemUtil.ensureLocationExists(newDescriptor, conf);

    // this object will be the table metadata
    Table table = HiveUtils.tableForDescriptor(
        namespace, name, newDescriptor, true /* external table */);

    // assign the location of the the table
    getMetaStoreUtil().createTable(table);

    return newDescriptor;
  }

  @Override
  public DatasetDescriptor update(String namespace, String name, DatasetDescriptor descriptor) {
    Compatibility.checkDatasetName(namespace, name);
    Compatibility.checkDescriptor(descriptor);

    String resolved = resolveNamespace(namespace, name);
    if (resolved != null) {
      return super.update(resolved, name, descriptor);
    }
    throw new DatasetNotFoundException(
        "Hive table not found: " + namespace + "." + name);
  }

  @Override
  public boolean delete(String namespace, String name) {
    Compatibility.checkDatasetName(namespace, name);
    String resolved = resolveNamespace(namespace, name);
    if (resolved != null) {
      return super.delete(resolved, name);
    }
    return false;
  }

  @Override
  public boolean exists(String namespace, String name) {
    Compatibility.checkDatasetName(namespace, name);
    return (resolveNamespace(namespace, name) != null);
  }

  @Override
  protected boolean isExternal(String namespace, String name) {
    String resolved = resolveNamespace(namespace, name);
    if (resolved != null) {
      return super.isExternal(resolved, name);
    }
    return false;
  }

  @Override
  protected boolean isManaged(String namespace, String name) {
    String resolved = resolveNamespace(namespace, name);
    if (resolved != null) {
      return super.isManaged(resolved, name);
    }
    return false;
  }

  private String resolveNamespace(String namespace, String name) {
    return resolveNamespace(namespace, name, null);
  }

  /**
   * Checks whether the Hive table {@code namespace.name} exists or if
   * {@code default.name} exists and should be used.
   *
   * @param namespace the requested namespace
   * @param name the table name
   * @param location the data location or null to use the default
   * @return if namespace.name exists, namespace. if not and default.name
   *          exists, then default. {@code null} otherwise.
   */
  private String resolveNamespace(String namespace, String name,
                                  @Nullable URI location) {
    if (getMetaStoreUtil().exists(namespace, name)) {
      return namespace;
    }
    try {
      DatasetDescriptor descriptor = HiveUtils.descriptorForTable(
          conf, getMetaStoreUtil().getTable(DEFAULT_NAMESPACE, name));
      URI expectedLocation;
      if (location == null) {
        expectedLocation = pathForDataset(namespace, name).toUri();
      } else {
        expectedLocation = location;
      }
      if (pathsEquivalent(expectedLocation, descriptor.getLocation())) {
        // table in the default db has the location that would have been used
        return DEFAULT_NAMESPACE;
      }
      // fall through and return null
    } catch (DatasetNotFoundException e) {
      // fall through and return null
    }
    return null;
  }

  private Path pathForDataset(String namespace, String name) {
    Preconditions.checkState(rootDirectory != null,
      "Dataset repository root directory can not be null");

    return rootFileSystem.makeQualified(
        HiveUtils.pathForDataset(rootDirectory, namespace, name));
  }

  private boolean pathsEquivalent(URI left, @Nullable URI right) {
    if (right == null) {
      return false;
    }
    String leftAuth = left.getAuthority();
    String rightAuth = right.getAuthority();
    if (leftAuth != null && rightAuth != null && !leftAuth.equals(rightAuth)) {
      // but authority sections are set, but do not match
      return false;
    }
    return (Objects.equal(left.getScheme(), right.getScheme()) &&
        Objects.equal(left.getPath(), right.getPath()));
  }
}
