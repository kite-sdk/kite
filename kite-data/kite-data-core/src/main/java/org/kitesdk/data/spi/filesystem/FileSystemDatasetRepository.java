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
package org.kitesdk.data.spi.filesystem;

import javax.annotation.Nullable;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.DatasetRepositoryException;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.IncompatibleSchemaException;
import org.kitesdk.data.PartitionKey;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.spi.AbstractDatasetRepository;
import org.kitesdk.data.spi.MetadataProvider;
import org.kitesdk.data.spi.PartitionListener;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.spi.SchemaUtil;
import org.kitesdk.data.spi.partition.IdentityFieldPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * A {@link org.kitesdk.data.DatasetRepository} that stores data in a Hadoop {@link FileSystem}.
 * </p>
 * <p>
 * Given a {@link FileSystem}, a root directory, and a {@link MetadataProvider},
 * this {@link org.kitesdk.data.DatasetRepository} implementation can load and store
 * {@link Dataset}s on both local filesystems as well as the Hadoop Distributed
 * FileSystem (HDFS). Users may directly instantiate this class with the three
 * dependencies above and then perform dataset-related operations using any of
 * the provided methods. The primary methods of interest will be
 * {@link #create(String, DatasetDescriptor)},
 * {@link #load(String)}, and
 * {@link #delete(String)} which create a new dataset, load an existing
 * dataset, or delete an existing dataset, respectively. Once a dataset has been created
 * or loaded, users can invoke the appropriate {@link Dataset} methods to get a reader
 * or writer as needed.
 * </p>
 * <p>
 * {@link org.kitesdk.data.DatasetWriter} instances returned from this
 * implementation have the following <code>flush()</code> method semantics. For Avro
 * files, <code>flush()</code> will invoke HDFS <code>hflush</code>,
 * which guarantees that client buffers are flushed, so new readers will see all
 * entries written up to that point. For Parquet files, <code>flush()</code> has no
 * effect.
 * </p>
 *
 * @see org.kitesdk.data.DatasetRepository
 * @see org.kitesdk.data.Dataset
 * @see org.kitesdk.data.DatasetDescriptor
 * @see org.kitesdk.data.PartitionStrategy
 * @see org.kitesdk.data.spi.MetadataProvider
 */
public class FileSystemDatasetRepository extends AbstractDatasetRepository {

  private static final Logger logger = LoggerFactory
    .getLogger(FileSystemDatasetRepository.class);

  private final MetadataProvider metadataProvider;
  private final Configuration conf;
  private final URI repositoryUri;

  /**
   * Construct a {@link FileSystemDatasetRepository} for the given
   * {@link MetadataProvider} for metadata storage.
   *
   * @param conf a {@link Configuration} for {@link FileSystem} access
   * @param metadataProvider the provider for metadata storage
   *
   * @since 0.8.0
   */
  public FileSystemDatasetRepository(
      Configuration conf, MetadataProvider metadataProvider) {
    this(conf, metadataProvider, null);
  }

  public FileSystemDatasetRepository(
      Configuration conf, MetadataProvider metadataProvider,
      @Nullable URI repositoryUri) {
    Preconditions.checkNotNull(conf, "Configuration cannot be null");
    Preconditions.checkNotNull(metadataProvider,
      "Metadata provider cannot be null");

    this.conf = conf;
    this.metadataProvider = metadataProvider;
    this.repositoryUri = repositoryUri;
  }

  @Override
  public <E> Dataset<E> create(String name, DatasetDescriptor descriptor) {
    Preconditions.checkNotNull(name, "Dataset name cannot be null");
    Preconditions.checkNotNull(descriptor, "Descriptor cannot be null");
    Preconditions.checkArgument(descriptor.getLocation() == null,
        "Descriptor location cannot be set; " +
        "it is assigned by the MetadataProvider");

    DatasetDescriptor newDescriptor = metadataProvider.create(name, descriptor);
    newDescriptor = addRepositoryUri(newDescriptor);

    final URI location = newDescriptor.getLocation();
    if (location == null) {
      throw new DatasetRepositoryException(
          "[BUG] MetadataProvider did not assign a location to dataset:" +
          name);
    }

    FileSystemUtil.ensureLocationExists(newDescriptor, conf);

    logger.debug("Created dataset:{} schema:{} datasetPath:{}", new Object[] {
        name, newDescriptor.getSchema(), location.toString() });

    return new FileSystemDataset.Builder()
        .name(name)
        .configuration(conf)
        .descriptor(newDescriptor)
        .partitionKey(newDescriptor.isPartitioned() ?
            org.kitesdk.data.impl.Accessor.getDefault().newPartitionKey() :
            null)
        .partitionListener(getPartitionListener())
        .build();
  }

  @Override
  public <E> Dataset<E> update(String name, DatasetDescriptor descriptor) {
    Preconditions.checkNotNull(name, "Dataset name cannot be null");
    Preconditions.checkNotNull(descriptor, "Descriptor cannot be null");

    DatasetDescriptor oldDescriptor = metadataProvider.load(name);

    // oldDescriptor is valid if load didn't throw NoSuchDatasetException

    if (!oldDescriptor.getFormat().equals(descriptor.getFormat())) {
      throw new DatasetRepositoryException("Cannot change dataset format from " +
          oldDescriptor.getFormat() + " to " + descriptor.getFormat());
    }

    final URI oldLocation = oldDescriptor.getLocation();
    if ((oldLocation != null) && !(oldLocation.equals(descriptor.getLocation()))) {
      throw new DatasetRepositoryException(
          "Cannot change the dataset's location");
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
    final Schema oldSchema = oldDescriptor.getSchema();
    final Schema newSchema = descriptor.getSchema();
    if (!SchemaValidationUtil.canRead(oldSchema, newSchema)) {
      throw new IncompatibleSchemaException("New schema cannot read data " +
          "written using " +
          "old schema. New schema: " + newSchema.toString(true) + "\nOld schema: " +
          oldSchema.toString(true));
    }

    DatasetDescriptor updatedDescriptor = metadataProvider.update(name, descriptor);
    updatedDescriptor = addRepositoryUri(updatedDescriptor);

    logger.debug("Updated dataset:{} schema:{} datasetPath:{}", new Object[] {
        name, updatedDescriptor.getSchema(),
        updatedDescriptor.getLocation().toString() });

    return new FileSystemDataset.Builder()
        .name(name)
        .configuration(conf)
        .descriptor(updatedDescriptor)
        .partitionKey(updatedDescriptor.isPartitioned() ?
            org.kitesdk.data.impl.Accessor.getDefault().newPartitionKey() :
            null)
        .partitionListener(getPartitionListener())
        .build();
  }

  @Override
  public <E> Dataset<E> load(String name) {
    Preconditions.checkNotNull(name, "Dataset name cannot be null");

    logger.debug("Loading dataset:{}", name);

    DatasetDescriptor descriptor = metadataProvider.load(name);
    descriptor = addRepositoryUri(descriptor);

    FileSystemDataset<E> ds = new FileSystemDataset.Builder()
        .name(name)
        .configuration(conf)
        .descriptor(descriptor)
        .partitionKey(descriptor.isPartitioned() ?
            org.kitesdk.data.impl.Accessor.getDefault().newPartitionKey() :
            null)
        .partitionListener(getPartitionListener())
        .build();

    logger.debug("Loaded dataset:{}", ds);

    return ds;
  }

  @Override
  public boolean delete(String name) {
    Preconditions.checkNotNull(name, "Dataset name cannot be null");

    logger.debug("Deleting dataset:{}", name);

    DatasetDescriptor descriptor;
    try {
      descriptor = metadataProvider.load(name);
      descriptor = addRepositoryUri(descriptor);
    } catch (DatasetNotFoundException ex) {
      return false;
    }

    boolean changed;
    try {
      // don't care about the return value here -- if it already doesn't exist
      // we still need to delete the data directory
      changed = metadataProvider.delete(name);
    } catch (DatasetException ex) {
      throw new DatasetRepositoryException(
          "Failed to delete descriptor for name:" + name, ex);
    }

    final Path dataLocation = new Path(descriptor.getLocation());
    final FileSystem fs = fsForPath(dataLocation, conf);

    try {
      if (fs.exists(dataLocation)) {
        if (fs.delete(dataLocation, true)) {
          changed = true;
        } else {
          throw new DatasetRepositoryException(
              "Failed to delete dataset name:" + name +
              " location:" + dataLocation);
        }
      }
    } catch (IOException e) {
      throw new DatasetRepositoryException(
          "Internal failure when removing location:" + dataLocation);
    }

    return changed;
  }

  @Override
  public boolean exists(String name) {
    Preconditions.checkNotNull(name, "Dataset name cannot be null");
    return metadataProvider.exists(name);
  }

  @Override
  public Collection<String> list() {
    return metadataProvider.list();
  }

  @Override
  public URI getUri() {
    return repositoryUri;
  }

  /**
   * Get a {@link org.kitesdk.data.PartitionKey} corresponding to a partition's filesystem path
   * represented as a {@link URI}. If the path is not a valid partition,
   * then {@link IllegalArgumentException} is thrown. Note that the partition does not
   * have to exist.
   * @param dataset the filesystem dataset
   * @param partitionPath a directory path where the partition data is stored
   * @return a partition key representing the partition at the given path
   * @since 0.4.0
   */
  @SuppressWarnings({"unchecked", "deprecation"})
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

    Schema schema = dataset.getDescriptor().getSchema();
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

      if (fp instanceof IdentityFieldPartitioner) {
        Class<? extends Object> expected = SchemaUtil.TYPE_TO_CLASS.get(
            schema.getField(fp.getSourceName()).schema().getType());
        values.add(fp.valueFromString(stringValue, expected));
      } else {
        values.add(fp.valueFromString(stringValue, fp.getType()));
      }
    }
    return org.kitesdk.data.impl.Accessor.getDefault().newPartitionKey(
        values.toArray(new Object[values.size()]));
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("metadataProvider", metadataProvider)
        .toString();
  }

  /**
   * @return the {@link MetadataProvider} being used by this repository.
   * @since 0.2.0
   */
  MetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  private PartitionListener getPartitionListener() {
    return metadataProvider instanceof PartitionListener ?
        (PartitionListener) metadataProvider : null;
  }

  private static FileSystem fsForPath(Path dataPath, Configuration conf) {
    try {
      return dataPath.getFileSystem(conf);
    } catch (IOException ex) {
      throw new DatasetRepositoryException(
          "Cannot get FileSystem for descriptor", ex);
    }
  }

  /**
   * A fluent builder to aid in the construction of {@link FileSystemDatasetRepository}
   * instances.
   * @since 0.2.0
   */
  public static class Builder {

    private Path rootDirectory;
    private FileSystem fileSystem;
    private Configuration configuration;

    /**
     * The root directory for metadata and dataset files.
     *
     * @param path a Path to a FileSystem location
     * @return this Builder for method chaining.
     */
    public Builder rootDirectory(Path path) {
      this.rootDirectory = path;
      return this;
    }

    /**
     * The root directory for metadata and dataset files.
     *
     * @param uri a URI to a FileSystem location
     * @return this Builder for method chaining.
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
      return rootDirectory(new URI(uri));
    }

    /**
     * The {@link FileSystem} to store metadata and dataset files in (optional).
     *
     * The FileSystem for the root directory is used if this FileSystem is not
     * set.
     */
    public Builder fileSystem(FileSystem fileSystem) {
      this.fileSystem = fileSystem;
      return this;
    }

    /**
     * The {@link Configuration} used to find the {@link FileSystem} (optional).
     * If not specified, the default configuration will be used.
     * @since 0.3.0
     */
    public Builder configuration(Configuration configuration) {
      this.configuration = configuration;
      return this;
    }

    /**
     * Build an instance of the configured {@link FileSystemDatasetRepository}.
     *
     * @since 0.9.0
     */
    public FileSystemDatasetRepository build() {
      URI repositoryUri = null;
      MetadataProvider metadataProvider = null;

      if (configuration == null) {
        this.configuration = new Configuration();
      }

      Preconditions.checkState(this.rootDirectory != null,
          "No root directory defined");

      // the rootDirectory can have a scheme/authority that overrides

      if (fileSystem != null) {
        // if the FS doesn't match, this will throw IllegalArgumentException
        Path qualifiedRootDirectory = fileSystem.makeQualified(rootDirectory);
        repositoryUri = makeRepositoryUri(qualifiedRootDirectory);
        metadataProvider = new FileSystemMetadataProvider(
            configuration, qualifiedRootDirectory);
      } else {
        repositoryUri = makeRepositoryUri(rootDirectory);
        metadataProvider = new FileSystemMetadataProvider(
            configuration, rootDirectory);
      }

      return new FileSystemDatasetRepository(configuration, metadataProvider,
          repositoryUri);
    }

    private URI makeRepositoryUri(Path rootDirectory) {
      return URI.create("repo:" + rootDirectory.toUri());
    }
  }
}
