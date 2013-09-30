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
import com.cloudera.cdk.data.DatasetExistsException;
import com.cloudera.cdk.data.DatasetRepositories;
import com.cloudera.cdk.data.DatasetRepository;
import com.cloudera.cdk.data.DatasetRepositoryException;
import com.cloudera.cdk.data.FieldPartitioner;
import com.cloudera.cdk.data.MetadataProvider;
import com.cloudera.cdk.data.MetadataProviderException;
import com.cloudera.cdk.data.NoSuchDatasetException;
import com.cloudera.cdk.data.spi.OptionBuilder;
import com.cloudera.cdk.data.PartitionKey;
import com.cloudera.cdk.data.PartitionStrategy;
import com.cloudera.cdk.data.spi.URIPattern;
import com.cloudera.cdk.data.filesystem.impl.Accessor;
import com.cloudera.cdk.data.spi.AbstractDatasetRepository;
import com.cloudera.cdk.data.spi.Loadable;
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
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Map;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.hadoop.fs.FileStatus;

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
        throw new DatasetExistsException(
            "Attempt to create an existing dataset:" + name);
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

    metadataProvider.create(name, descriptor);

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

    metadataProvider.update(name, descriptor);

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
    checkExists(fileSystem, datasetDirectory);

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
      // don't care about the return value here -- if it already doesn't exist
      // we still need to delete the data directory
      metadataProvider.delete(name);
    } catch (MetadataProviderException ex) {
      throw new DatasetRepositoryException(
          "Failed to delete descriptor for name:" + name, ex);
    }

    try {
      if (fileSystem.exists(datasetPath)) {
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

  @Override
  public boolean exists(String name) {
    final Path potentialPath = pathForDataset(name);
    try {
      return (
          fileSystem.exists(potentialPath) &&
          fileSystem.isDirectory(rootDirectory));
    } catch (IOException ex) {
      throw new DatasetRepositoryException(
          "Could not check data path:" + potentialPath, ex);
    }
  }

  @Override
  public Collection<String> list() {
    List<String> datasets = Lists.newArrayList();
    try {
      FileStatus[] entries = fileSystem.listStatus(rootDirectory,
          PathFilters.notHidden());
      for (FileStatus entry : entries) {
        // assumes that all unhidden directories under the root are data sets
        if (entry.isDirectory()) {
          // may want to add a check: !RESERVED_NAMES.contains(name)
          datasets.add(entry.getPath().getName());
        } else {
          continue;
        }
      }
    } catch (IOException ex) {
      throw new DatasetRepositoryException("Could not list data sets", ex);
    }
    return datasets;
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

    return pathForDataset(rootDirectory, name);
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
   * Returns the correct dataset path for the given name and root directory.
   *
   * @param root A Path
   * @param name A String dataset name
   * @return the correct dataset Path
   * @since 0.7.0
   */
  protected static Path pathForDataset(Path root, String name) {
    Preconditions.checkArgument(name != null, "Dataset name cannot be null");

    // Why replace '.' here? Is this a namespacing hack?
    return new Path(root, name.replace('.', Path.SEPARATOR_CHAR));
  }

  /**
   * Precondition-style static validation that a dataset exists
   *
   * @param fs        A FileSystem where the data should be stored
   * @param location  The Path where the data should be stored
   * @throws NoSuchDatasetException     if the data location is missing
   * @throws DatasetRepositoryException if any IOException is thrown
   * @since 0.7.0
   */
  protected static void checkExists(FileSystem fs, Path location) {
    try {
      if (!fs.exists(location)) {
        throw new NoSuchDatasetException("Data location is missing");
      }
    } catch (IOException ex) {
      throw new DatasetRepositoryException("Cannot access data location", ex);
    }
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

  /**
   * This class builds configured instances of
   * {@code FileSystemDatasetRepository} from a Map of options. This is for the
   * URI system.
   */
  private static class URIBuilder implements OptionBuilder<DatasetRepository> {

    private final Configuration envConf;

    public URIBuilder(Configuration envConf) {
      this.envConf = envConf;
    }

    @Override
    public DatasetRepository getFromOptions(Map<String, String> match) {
      final MapConfiguration options = new MapConfiguration(match);
      final Path root;
      String path = options.getString("path");
      if (path == null || path.isEmpty()) {
        root = new Path("/");
      } else if (options.getBoolean("absolute", false)) {
        root = new Path("/", path);
      } else {
        root = new Path(path);
      }
      final FileSystem fs;
      try {
        fs = FileSystem.get(fileSystemURI(options), envConf);
      } catch (IOException ex) {
        throw new DatasetRepositoryException(
            "Could not get a FileSystem", ex);
      }
      return (DatasetRepository) new FileSystemDatasetRepository.Builder()
          .rootDirectory(root)
          .fileSystem(fs)
          .get();
    }

    private URI fileSystemURI(MapConfiguration options) {
      final String userInfo;
      if (options.containsKey("username")) {
        if (options.containsKey("password")) {
          userInfo = options.getString("password") + ":" +
              options.getString("username");
        } else {
          userInfo = options.getString("username");
        }
      } else {
        userInfo = null;
      }
      try {
        return new URI(options.getString("scheme"), userInfo,
            options.getString("host"), options.getInteger("port", -1),
            "/", null, null);
      } catch (URISyntaxException ex) {
        throw new DatasetRepositoryException("Could not build FS URI", ex);
      }
    }
  }

  /**
   * A Loader implementation to register URIs for FileSystemDatasetRepositories.
   */
  public static class Loader implements Loadable {
    @Override
    public void load() {
      // get a default Configuration to configure defaults (so it's okay!)
      final Configuration conf = new Configuration();
      final OptionBuilder<DatasetRepository> builder = new URIBuilder(conf);

      DatasetRepositories.register(
          new URIPattern(URI.create("file:*path")), builder);
      DatasetRepositories.register(
          new URIPattern(URI.create("file:/*path?absolute=true")), builder);

      String hdfsAuthority;
      try {
        // Use a HDFS URI with no authority and the environment's configuration
        // to find the default HDFS information
        final URI hdfs = FileSystem.get(URI.create("hdfs:/"), conf).getUri();
        hdfsAuthority = hdfs.getAuthority();
      } catch (IOException ex) {
        logger.warn(
            "Could not locate HDFS, host and port will not be set by default.");
        hdfsAuthority = "";
      }

      DatasetRepositories.register(
          new URIPattern(URI.create(
              "hdfs://" + hdfsAuthority + "/*path?absolute=true")),
          builder);
    }
  }
}
