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
package com.cloudera.cdk.data.filesystem;

import com.cloudera.cdk.data.Dataset;
import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.DatasetRepository;
import com.cloudera.cdk.data.DatasetRepositoryException;
import com.cloudera.cdk.data.FieldPartitioner;
import com.cloudera.cdk.data.MetadataProvider;
import com.cloudera.cdk.data.PartitionKey;
import com.cloudera.cdk.data.PartitionStrategy;
import com.cloudera.cdk.data.filesystem.impl.Accessor;
import com.cloudera.cdk.data.spi.AbstractDatasetRepository;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * <p>
 * A {@link com.cloudera.cdk.data.DatasetRepository} that stores data in a Hadoop {@link FileSystem}.
 * </p>
 * <p>
 * Given a {@link FileSystem}, a root directory, and a {@link com.cloudera.cdk.data.MetadataProvider},
 * this {@link com.cloudera.cdk.data.DatasetRepository} implementation can load and store
 * {@link com.cloudera.cdk.data.Dataset}s on both local filesystems as well as the Hadoop Distributed
 * FileSystem (HDFS). Users may directly instantiate this class with the three
 * dependencies above and then perform dataset-related operations using any of
 * the provided methods. The primary methods of interest will be
 * {@link #create(String, com.cloudera.cdk.data.DatasetDescriptor)}, {@link #get(String)}, and
 * {@link #drop(String)} which create a new dataset, load an existing
 * dataset, or delete an existing dataset, respectively. Once a dataset has been created
 * or loaded, users can invoke the appropriate {@link com.cloudera.cdk.data.Dataset} methods to get a reader
 * or writer as needed.
 * </p>
 *
 * @see com.cloudera.cdk.data.DatasetRepository
 * @see com.cloudera.cdk.data.Dataset
 * @see com.cloudera.cdk.data.DatasetDescriptor
 * @see com.cloudera.cdk.data.PartitionStrategy
 * @see com.cloudera.cdk.data.MetadataProvider
 */
public class FileSystemDatasetRepository extends AbstractDatasetRepository {

  private static final Logger logger = LoggerFactory
    .getLogger(FileSystemDatasetRepository.class);

  static {
    Accessor.setDefault(new AccessorImpl());
  }

  private final MetadataProvider metadataProvider;

  private final Path rootDirectory;
  private final FileSystem fileSystem;

  /**
   * Construct a {@link FileSystemDatasetRepository} on the given {@link FileSystem} and
   * root directory, and a {@link FileSystemMetadataProvider} with the same {@link
   * FileSystem} and root directory.
   *
   * @param fileSystem    the filesystem to store metadata and datasets in
   * @param rootDirectory the root directory for metadata and datasets
   */
  public FileSystemDatasetRepository(FileSystem fileSystem, Path rootDirectory) {
    this(fileSystem, rootDirectory,
      new FileSystemMetadataProvider(fileSystem, rootDirectory));
  }

  /**
   * Construct a {@link FileSystemDatasetRepository} with a root directory at the
   * given {@link URI}, and a {@link FileSystemMetadataProvider} with the same root
   * directory.
   *
   * @param uri the root directory for metadata and datasets
   * @since 0.3.0
   */
  public FileSystemDatasetRepository(URI uri) {
    Preconditions.checkArgument(uri != null,
        "URI provider can not be null");

    this.rootDirectory = new Path(uri);
    try {
      fileSystem = rootDirectory.getFileSystem(new Configuration());
    } catch (IOException e) {
      throw new DatasetRepositoryException("Problem creating " +
          "FileSystemDatasetRepository.", e);
    }
    this.metadataProvider = new FileSystemMetadataProvider(fileSystem, rootDirectory);
  }

  /**
   * Construct a {@link FileSystemDatasetRepository} on the given {@link FileSystem} and
   * root directory, with the given {@link MetadataProvider} for metadata storage.
   *
   * @param fileSystem       the filesystem to store datasets in
   * @param rootDirectory    the root directory for datasets
   * @param metadataProvider the provider for metadata storage
   */
  public FileSystemDatasetRepository(FileSystem fileSystem, Path rootDirectory,
    MetadataProvider metadataProvider) {

    Preconditions.checkArgument(fileSystem != null,
      "FileSystem can not be null");
    Preconditions.checkArgument(rootDirectory != null,
      "Root directory can not be null");
    Preconditions.checkArgument(metadataProvider != null,
      "Metadata provider can not be null");

    this.fileSystem = fileSystem;
    this.rootDirectory = rootDirectory;
    this.metadataProvider = metadataProvider;
  }

  @Override
  public Dataset create(String name, DatasetDescriptor descriptor) {

    Preconditions.checkArgument(name != null, "Name can not be null");
    Preconditions.checkArgument(descriptor != null,
      "Descriptor can not be null");

    Schema schema = descriptor.getSchema();
    Path datasetPath = pathForDataset(name);

    try {
      if (fileSystem.exists(datasetPath)) {
        throw new DatasetRepositoryException("Attempt to create an existing dataset:" + name);
      }
    } catch (IOException e) {
      throw new DatasetRepositoryException("Internal error while determining if dataset path already exists:" + datasetPath, e);
    }

    logger.debug("Creating dataset:{} schema:{} datasetPath:{}", new Object[] {
      name, schema, datasetPath });

    try {
      if (!fileSystem.mkdirs(datasetPath)) {
        throw new DatasetRepositoryException("Failed to make dataset path:" + datasetPath);
      }
    } catch (IOException e) {
      throw new DatasetRepositoryException("Internal failure while creating dataset path:" + datasetPath, e);
    }

    metadataProvider.save(name, descriptor);

    return new FileSystemDataset.Builder()
      .name(name)
      .fileSystem(fileSystem)
      .descriptor(descriptor)
      .directory(pathForDataset(name))
      .partitionKey(
        descriptor.isPartitioned() ? com.cloudera.cdk.data.impl.Accessor.getDefault()
          .newPartitionKey() : null).get();
  }

  @Override
  public Dataset update(String name, DatasetDescriptor descriptor) {
    DatasetDescriptor oldDescriptor = metadataProvider.load(name);

    if (!oldDescriptor.getFormat().equals(descriptor.getFormat())) {
      throw new DatasetRepositoryException("Cannot change dataset format from " +
          oldDescriptor.getFormat() + " to " + descriptor.getFormat());
    }

    if (oldDescriptor.isPartitioned() != descriptor.isPartitioned()) {
      throw new DatasetRepositoryException("Cannot change an unpartitioned dataset to " +
          " partitioned or vice versa.");
    } else if (oldDescriptor.isPartitioned() && descriptor.isPartitioned() &&
        !oldDescriptor.getPartitionStrategy().equals(descriptor.getPartitionStrategy())) {
      throw new DatasetRepositoryException("Cannot change partition strategy from " +
          oldDescriptor.getPartitionStrategy() + " to " + descriptor.getPartitionStrategy());
    }

    // check can read records written with old schema using new schema
    Schema oldSchema = oldDescriptor.getSchema();
    Schema newSchema = descriptor.getSchema();
    if (!SchemaValidationUtil.canRead(oldSchema, newSchema)) {
      throw new DatasetRepositoryException("New schema cannot read data written using " +
          "old schema. New schema: " + newSchema.toString(true) + "\nOld schema: " +
          oldSchema.toString(true));
    }

    metadataProvider.save(name, descriptor);

    return new FileSystemDataset.Builder()
        .name(name)
        .fileSystem(fileSystem)
        .descriptor(descriptor)
        .directory(pathForDataset(name))
        .partitionKey(
            descriptor.isPartitioned() ? com.cloudera.cdk.data.impl.Accessor.getDefault()
                .newPartitionKey() : null).get();
  }

  @Override
  public Dataset load(String name) {
    Preconditions.checkArgument(name != null, "Name can not be null");

    logger.debug("Loading dataset:{}", name);

    Path datasetDirectory = pathForDataset(name);

    DatasetDescriptor descriptor = metadataProvider.load(name);

    FileSystemDataset ds = new FileSystemDataset.Builder()
      .fileSystem(fileSystem).descriptor(descriptor)
      .directory(datasetDirectory).name(name).get();

    logger.debug("Loaded dataset:{}", ds);

    return ds;
  }

  @Override
  public boolean delete(String name) {
    Preconditions.checkArgument(name != null, "Name can not be null");

    logger.debug("Dropping dataset:{}", name);

    Path datasetPath = pathForDataset(name);

    try {
      if (metadataProvider.delete(name) && fileSystem.exists(datasetPath)) {
        if (fileSystem.delete(datasetPath, true)) {
          return true;
        } else {
          throw new DatasetRepositoryException("Failed to delete dataset name:" + name
            + " data path:" + datasetPath);
        }
      } else {
        return false;
      }
    } catch (IOException e) {
      throw new DatasetRepositoryException("Internal failure to test if dataset path exists:" + datasetPath);
    }
  }

  /**
   * Get a {@link com.cloudera.cdk.data.PartitionKey} corresponding to a partition's filesystem path
   * represented as a {@link URI}. If the path is not a valid partition,
   * then {@link IllegalArgumentException} is thrown. Note that the partition does not
   * have to exist.
   * @param dataset the filesystem dataset
   * @param partitionPath a directory path where the partition data is stored
   * @return a partition key representing the partition at the given path
   * @since 0.4.0
   */
  public static PartitionKey partitionKeyForPath(Dataset dataset, URI partitionPath) {
    Preconditions.checkState(dataset.getDescriptor().isPartitioned(),
        "Attempt to get a partition on a non-partitioned dataset (name:%s)",
        dataset.getName());

    Preconditions.checkArgument(dataset instanceof FileSystemDataset,
        "Dataset is not a FileSystemDataset");
    FileSystemDataset fsDataset = (FileSystemDataset) dataset;

    FileSystem fs = fsDataset.getFileSystem();
    URI partitionUri = fs.makeQualified(new Path(partitionPath)).toUri();
    URI directoryUri = fsDataset.getDirectory().toUri();
    URI relativizedUri = directoryUri.relativize(partitionUri);

    if (relativizedUri.equals(partitionUri)) {
      throw new IllegalArgumentException(String.format("Partition URI %s has different " +
          "root directory to dataset (directory: %s).", partitionUri, directoryUri));
    }

    Iterable<String> parts = Splitter.on('/').split(relativizedUri.getPath());

    PartitionStrategy partitionStrategy = dataset.getDescriptor().getPartitionStrategy();
    List<FieldPartitioner> fieldPartitioners = partitionStrategy.getFieldPartitioners();
    if (Iterables.size(parts) > fieldPartitioners.size()) {
      throw new IllegalArgumentException(String.format("Too many partition directories " +
          "for %s (%s), expecting %s.", partitionUri, Iterables.size(parts),
          fieldPartitioners.size()));
    }

    List<Object> values = Lists.newArrayList();
    int i = 0;
    for (String part : parts) {
      Iterator<String> split = Splitter.on('=').split(part).iterator();
      String fieldName = split.next();
      FieldPartitioner fp = fieldPartitioners.get(i++);
      if (!fieldName.equals(fp.getName())) {
        throw new IllegalArgumentException(String.format("Unrecognized partition name " +
            "'%s' in partition %s, expecting '%s'.", fieldName, partitionUri,
            fp.getName()));
      }
      if (!split.hasNext()) {
        throw new IllegalArgumentException(String.format("Missing partition value for " +
            "'%s' in partition %s.", fieldName, partitionUri));
      }
      String stringValue = split.next();
      Object value = fp.valueFromString(stringValue);
      values.add(value);
    }
    return com.cloudera.cdk.data.impl.Accessor.getDefault().newPartitionKey(
        values.toArray(new Object[values.size()]));
  }

  /**
   * <p>
   * Implementations should return the fully-qualified path of the data directory for
   * the dataset with the given name.
   * </p>
   * <p>
   * This method is for internal use only and users should not call it directly.
   * </p>
   * @since 0.2.0
   */
  protected Path pathForDataset(String name) {
    Preconditions.checkState(rootDirectory != null,
      "Dataset repository root directory can not be null");

    return new Path(rootDirectory, name.replace('.', '/'));
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("rootDirectory", rootDirectory)
      .add("metadataProvider", metadataProvider)
      .add("fileSystem", fileSystem).toString();
  }

  /**
   * @return the root directory in the filesystem where datasets are stored.
   */
  public Path getRootDirectory() {
    return rootDirectory;
  }

  /**
   * @return the {@link FileSystem} on which datasets are stored.
   */
  public FileSystem getFileSystem() {
    return fileSystem;
  }

  /**
   * @return the {@link MetadataProvider} being used by this repository.
   * @since 0.2.0
   */
  public MetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  /**
   * A fluent builder to aid in the construction of {@link FileSystemDatasetRepository}
   * instances.
   * @since 0.2.0
   */
  public static class Builder implements Supplier<FileSystemDatasetRepository> {

    private FileSystem fileSystem;
    private Path rootDirectory;
    private MetadataProvider metadataProvider;
    private Configuration configuration;

    /**
     * The root directory for metadata and dataset files.
     */
    public Builder rootDirectory(Path path) {
      this.rootDirectory = path;
      return this;
    }

    /**
     * The root directory for metadata and dataset files.
     */
    public Builder rootDirectory(URI uri) {
      this.rootDirectory = new Path(uri);
      return this;
    }

    /**
     * The {@link FileSystem} to store metadata and dataset files in. Optional. If not
     * specified, the default filesystem will be used.
     */
    public Builder fileSystem(FileSystem fileSystem) {
      this.fileSystem = fileSystem;
      return this;
    }

    /**
     * The {@link MetadataProvider} for metadata storage. Optional. If not
     * specified, a {@link FileSystemMetadataProvider} will be used.
     */
    public Builder metadataProvider(MetadataProvider metadataProvider) {
      this.metadataProvider = metadataProvider;
      return this;
    }

    /**
     * The {@link Configuration} used to find the {@link FileSystem}. Optional. If not
     * specified, the default configuration will be used.
     * @since 0.3.0
     */
    public Builder configuration(Configuration configuration) {
      this.configuration = configuration;
      return this;
    }

    @Override
    public FileSystemDatasetRepository get() {
      Preconditions.checkState(this.rootDirectory != null, "No root directory defined");

      if (fileSystem == null) {
        if (configuration == null) {
          configuration = new Configuration();
        }
        try {
          fileSystem = rootDirectory.getFileSystem(configuration);
        } catch (IOException e) {
          throw new DatasetRepositoryException("Problem creating " +
              "FileSystemDatasetRepository.", e);
        }
      }

      if (metadataProvider == null) {
        metadataProvider = new FileSystemMetadataProvider(fileSystem, rootDirectory);
      }

      return new FileSystemDatasetRepository(fileSystem, rootDirectory, metadataProvider);
    }
  }

}
