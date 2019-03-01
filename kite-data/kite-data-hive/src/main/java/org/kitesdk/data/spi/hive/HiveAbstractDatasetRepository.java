/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.spi.hive;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.net.URI;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.spi.filesystem.FileSystemDatasetRepository;
import org.kitesdk.data.spi.MetadataProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HiveAbstractDatasetRepository extends FileSystemDatasetRepository {

  private static final Logger LOG = LoggerFactory
      .getLogger(HiveAbstractDatasetRepository.class);

  private static final String HIVE_METASTORE_URIS_SEPARATOR = ",";

  private final MetadataProvider provider;
  private final URI repoUri;

  private static Path getRootDirectory(Configuration conf) {
    String pathString = conf.get("kite.hive.tmp.root", "/tmp");
    LOG.debug("Using root directory: " + pathString);
    return new Path(pathString);
  }

  /**
   * Create an HCatalog dataset repository with external tables.
   */
  HiveAbstractDatasetRepository(Configuration conf, Path rootDirectory,
                                MetadataProvider provider) {
    super(conf, rootDirectory, provider);
    this.provider = provider;
    this.repoUri = getRepositoryUri(conf, rootDirectory);
  }

  /**
   * Create an HCatalog dataset repository with managed tables.
   */
  HiveAbstractDatasetRepository(Configuration conf, MetadataProvider provider) {
    // Because the managed provider overrides dataset locations, the only time
    // the storage path is used is to create temporary dataset repositories
    super(conf, getRootDirectory(conf), provider);
    this.provider = provider;
    this.repoUri = getRepositoryUri(conf, null);
  }

  @Override
  public boolean delete(String namespace, String name) {
    try {
      if (isManaged(namespace, name)) {
        // avoids calling fsRepository.delete, which deletes the data path
        return getMetadataProvider().delete(namespace, name);
      }
      return super.delete(namespace, name);
    } catch (DatasetNotFoundException e) {
      return false;
    }
  }

  @Override
  public boolean moveToTrash(String namespace, String name) {
    try {
      if (isManaged(namespace, name)) {
        // avoids calling fsRepository.delete, which deletes the data path
        // managed tables by default go to trash if it is enabled so call delete
        return getMetadataProvider().delete(namespace, name);
      }
      return super.moveToTrash(namespace, name);
    } catch (DatasetNotFoundException e) {
      return false;
    }
  }

  @Override
  public URI getUri() {
    return repoUri;
  }

  @VisibleForTesting
  MetadataProvider getMetadataProvider() {
    return provider;
  }

  private boolean isManaged(String namespace, String name) {
    MetadataProvider provider = getMetadataProvider();
    if (provider instanceof HiveAbstractMetadataProvider) {
      return ((HiveAbstractMetadataProvider) provider).isManaged(namespace, name);
    }
    // if the provider isn't talking to Hive, then it isn't managed
    return false;
  }

  private URI getRepositoryUri(Configuration conf,
                               @Nullable Path rootDirectory) {
    String hiveMetaStoreUriProperty = getHiveMetastoreUri(conf);
    StringBuilder uri = new StringBuilder("repo:hive");
    if (hiveMetaStoreUriProperty != null) {
      URI hiveMetaStoreUri = URI.create(hiveMetaStoreUriProperty);
      Preconditions.checkArgument(hiveMetaStoreUri.getScheme().equals("thrift"),
          "Metastore URI scheme must be 'thrift'.");
      uri.append("://").append(hiveMetaStoreUri.getAuthority());
    }
    if (rootDirectory != null) {
      if (hiveMetaStoreUriProperty == null) {
        uri.append(":");
      }
      URI rootUri = rootDirectory.toUri();
      uri.append(rootUri.getPath());
      if (rootUri.getHost() != null) {
        uri.append("?").append("hdfs:host").append("=").append(rootUri.getHost());
        if (rootUri.getPort() != -1) {
          uri.append("&").append("hdfs:port").append("=").append(rootUri.getPort());
        }
      }
    }
    return URI.create(uri.toString());
  }

  /**
   * This method extracts one URI for the Hive metastore. The hive.metastore.uris property in the parameter
   * Configuration object can contain a list of uris but since Kite does not support highly available Hive metastore
   * currently we need to make sure that only the first one is retrieved.
   *
   * @param conf The Configuration object potentially containing the hive.metastore.uris property.
   * @return The first URI from the hive.metastore.uris property if it is set, null otherwise.
   */
  String getHiveMetastoreUri(Configuration conf) {
    String metastoreUris = conf.get(Loader.HIVE_METASTORE_URI_PROP);
    if (metastoreUris == null) {
      return null;
    }

    String[] uriArray = metastoreUris.split(HIVE_METASTORE_URIS_SEPARATOR);

    return uriArray[0];
  }
}
