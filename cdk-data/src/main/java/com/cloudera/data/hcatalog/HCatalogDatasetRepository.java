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
package com.cloudera.data.hcatalog;

import com.cloudera.data.Dataset;
import com.cloudera.data.DatasetDescriptor;
import com.cloudera.data.DatasetRepository;
import com.cloudera.data.DatasetRepositoryException;
import com.cloudera.data.MetadataProvider;
import com.cloudera.data.filesystem.FileSystemDatasetRepository;
import com.google.common.base.Supplier;
import java.io.IOException;
import java.net.URI;
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
 * {@link #create(String, DatasetDescriptor)}, {@link #get(String)}, and
 * {@link #drop(String)} which create a new dataset, load an existing
 * dataset, or delete an existing dataset, respectively. Once a dataset has been created
 * or loaded, users can invoke the appropriate {@link Dataset} methods to get a reader
 * or writer as needed.
 * </p>
 *
 * @see DatasetRepository
 * @see Dataset
 */
public class HCatalogDatasetRepository implements DatasetRepository {

  private final boolean managed;

  private final HCatalogMetadataProvider metadataProvider;
  private FileSystemDatasetRepository fileSystemDatasetRepository;

  /**
   * <p>
   * Create an HCatalog dataset repository with managed tables. The location of the
   * data directory is determined by the setting of
   * <code>hive.metastore.warehouse.dir</code>, read from <i>hive-site.xml</i> on the
   * classpath, or if no such file is found then the default for this property.
   * </p>
   */
  public HCatalogDatasetRepository() {
    this.managed = true;
    this.metadataProvider = new HCatalogMetadataProvider(managed);
  }

  /**
   * <p>
   * Create an HCatalog dataset repository with external tables.
   * </p>
   * @param uri the root directory for datasets
   * @since 0.3.0
   */
  public HCatalogDatasetRepository(URI uri) {
    this.managed = false; // TODO depends
    this.metadataProvider = new HCatalogMetadataProvider(managed);

    this.fileSystemDatasetRepository = new FileSystemDatasetRepository.Builder()
        .rootDirectory(uri).metadataProvider(metadataProvider).get();
  }

  /**
   * <p>
   * Create an HCatalog dataset repository with external tables.
   * </p>
   * @param fileSystem    the filesystem to store datasets in
   * @param rootDirectory the root directory for datasets
   */
  public HCatalogDatasetRepository(FileSystem fileSystem, Path rootDirectory) {
    this.managed = false;
    this.metadataProvider = new HCatalogMetadataProvider(managed);

    this.fileSystemDatasetRepository = new FileSystemDatasetRepository(fileSystem,
        rootDirectory, metadataProvider);
  }

  @Override
  public Dataset create(String name, DatasetDescriptor descriptor) {
    if (managed) {
      // HCatalog handles data directory creation
      metadataProvider.save(name, descriptor);
      if (fileSystemDatasetRepository == null) {
        fileSystemDatasetRepository = new FileSystemDatasetRepository(
            metadataProvider.getFileSystem(),
            /* unused */ metadataProvider.getDataDirectory().getParent(),
            metadataProvider) {
          @Override
          protected Path pathForDataset(String name) {
            return metadataProvider.getDataDirectory();
          }
        };
      }
      return fileSystemDatasetRepository.get(name);
    } else {
      metadataProvider.setFileSystem(fileSystemDatasetRepository.getFileSystem());
      Path dataDir = new Path(fileSystemDatasetRepository.getRootDirectory(), name);
      metadataProvider.setDataDirectory(dataDir);
      return fileSystemDatasetRepository.create(name, descriptor);
    }
  }

  @Override
  public Dataset update(String name, DatasetDescriptor descriptor) {
    throw new DatasetRepositoryException("Descriptor updates are not supported.");
  }

  @Override
  public Dataset get(String name) {
    if (managed && fileSystemDatasetRepository == null) {
      metadataProvider.load(name);
      fileSystemDatasetRepository = new FileSystemDatasetRepository(
          metadataProvider.getFileSystem(),
            /* unused */ metadataProvider.getDataDirectory().getParent(),
          metadataProvider) {
        @Override
        protected Path pathForDataset(String name) {
          return metadataProvider.getDataDirectory();
        }
      };
    }
    return fileSystemDatasetRepository.get(name);
  }

  @Override
  public boolean drop(String name) {
    if (managed) {
      // HCatalog handles data directory deletion
      return metadataProvider.delete(name);
    } else {
      return fileSystemDatasetRepository.drop(name);
    }
  }

  /**
   * A fluent builder to aid in the construction of {@link HCatalogDatasetRepository}
   * instances.
   * @since 0.3.0
   */
  public static class Builder implements Supplier<HCatalogDatasetRepository> {

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
     * The {@link FileSystem} to store dataset files in. Optional. If not
     * specified, the default filesystem will be used.
     */
    public Builder fileSystem(FileSystem fileSystem) {
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
    public HCatalogDatasetRepository get() {

      if (rootDirectory == null) {
        return new HCatalogDatasetRepository();
      }

      if (fileSystem == null) {
        if (configuration == null) {
          configuration = new Configuration();
        }
        try {
          fileSystem = rootDirectory.getFileSystem(configuration);
        } catch (IOException e) {
          throw new DatasetRepositoryException("Problem creating " +
              "HCatalogDatasetRepository.", e);
        }
      }

      return new HCatalogDatasetRepository(fileSystem, rootDirectory);
    }
  }
}
