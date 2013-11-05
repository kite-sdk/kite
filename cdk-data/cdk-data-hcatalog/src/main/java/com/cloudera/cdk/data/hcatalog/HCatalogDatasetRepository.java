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
package com.cloudera.cdk.data.hcatalog;

import com.cloudera.cdk.data.Dataset;
import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.DatasetRepository;
import com.cloudera.cdk.data.MetadataProvider;
import com.cloudera.cdk.data.filesystem.FileSystemDatasetRepository;
import com.cloudera.cdk.data.spi.AbstractDatasetRepository;
import com.google.common.base.Supplier;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * <p>
 * A {@link DatasetRepository} that uses the Hive/HCatalog metastore for metadata,
 * and stores data in a Hadoop {@link FileSystem}.
 * </p>
 * <p>
 * The location of the data directory is either chosen by Hive/HCatalog (so called
 * "managed tables"), or specified when creating an instance of this class by providing
 * a {@link FileSystem}, and a root directory in the constructor ("external tables").
 * </p>
 * <p>
 * The primary methods of interest will be
 * {@link #create(String, DatasetDescriptor)}, {@link #load(String)}, and
 * {@link #delete(String)} which create a new dataset, load an existing
 * dataset, or delete an existing dataset, respectively. Once a dataset has been created
 * or loaded, users can invoke the appropriate {@link Dataset} methods to get a reader
 * or writer as needed.
 * </p>
 *
 * @see DatasetRepository
 * @see Dataset
 */
public class HCatalogDatasetRepository extends AbstractDatasetRepository {

  private final MetadataProvider metadataProvider;
  private final FileSystemDatasetRepository fsRepository;

  /**
   * Create an HCatalog dataset repository with managed tables.
   */
  HCatalogDatasetRepository(Configuration conf, MetadataProvider provider) {
    this.metadataProvider = provider;
    this.fsRepository = new FileSystemDatasetRepository.Builder().configuration(conf)
        .metadataProvider(metadataProvider).build();
  }

  @Override
  public <E> Dataset<E> create(String name, DatasetDescriptor descriptor) {
    // avoids calling fsRepository.create, which creates the data path
    metadataProvider.create(name, descriptor);
    return fsRepository.load(name);
  }

  @Override
  public <E> Dataset<E> update(String name, DatasetDescriptor descriptor) {
    throw new UnsupportedOperationException(
        "Descriptor updates are not supported.");
  }

  @Override
  public <E> Dataset<E> load(String name) {
    return fsRepository.load(name);
  }

  @Override
  public boolean delete(String name) {
    // avoids calling fsRepository.delete, which deletes the data path
    return metadataProvider.delete(name);
  }

  @Override
  public boolean exists(String name) {
    return metadataProvider.exists(name);
  }

  @Override
  public Collection<String> list() {
    return metadataProvider.list();
  }

  /**
   * Returns the {@link MetadataProvider} used by this repository.
   *
   * @return the MetadataProvider used by this repository.
   */
  MetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  /**
   * A fluent builder to aid in the construction of {@link HCatalogDatasetRepository}
   * instances.
   * @since 0.3.0
   */
  public static class Builder implements Supplier<DatasetRepository> {

    private FileSystem fileSystem;
    private Path rootDirectory;
    private Configuration configuration;

    /**
     * The root directory for dataset files.
     */
    public Builder rootDirectory(Path path) {
      this.rootDirectory = path;
      return this;
    }

    /**
     * The root directory for dataset files.
     */
    public Builder rootDirectory(URI uri) {
      this.rootDirectory = new Path(uri);
      return this;
    }

    /**
     * The root directory for metadata and dataset files.
     *
     * @param uri a String to parse as a URI
     * @return this Builder for method chaining.
     * @throws URISyntaxException
     *
     * @since 0.8.0
     */
    public Builder rootDirectory(String uri) throws URISyntaxException {
      this.rootDirectory = new Path(new URI(uri));
      return this;
    }

    /**
     * The {@link FileSystem} to store dataset files in (ignored).
     *
     * Calls to this method are ignored because Hive requires using HDFS.
     *
     * @deprecated will be removed in 0.9.0
     */
    @Deprecated
    public Builder fileSystem(FileSystem fileSystem) {
      // This method will be removed because a Hive-compatible FS must be used
      this.fileSystem = fileSystem;
      return this;
    }

    /**
     * The {@link Configuration} used to find the {@link FileSystem}. Optional. If not
     * specified, the default configuration will be used.
     */
    public Builder configuration(Configuration configuration) {
      this.configuration = configuration;
      return this;
    }

    @Override
    public DatasetRepository get() {

      if (configuration == null) {
        this.configuration = new Configuration();
      }

      if (rootDirectory != null) {
        // external
        if (fileSystem != null) {
          // this will throw IllegalArgumentException if rootDirectory's FS
          // doesn't match the fileSystem that is set.
          HCatalogMetadataProvider metadataProvider =
              new HCatalogExternalMetadataProvider(
                  configuration, fileSystem.makeQualified(rootDirectory));
          return new FileSystemDatasetRepository.Builder().configuration(configuration)
              .metadataProvider(metadataProvider).build();
        } else {
          HCatalogMetadataProvider metadataProvider =
              new HCatalogExternalMetadataProvider(
                  configuration, rootDirectory);
          return new FileSystemDatasetRepository.Builder().configuration(configuration)
              .metadataProvider(metadataProvider).build();
        }
      } else {
        // managed
        HCatalogMetadataProvider metadataProvider =
            new HCatalogManagedMetadataProvider(configuration);
        return new HCatalogDatasetRepository(configuration, metadataProvider);
      }
    }
  }
}
