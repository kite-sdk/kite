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
import org.kitesdk.data.spi.Compatibility;
import org.kitesdk.data.spi.PartitionKey;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.spi.FieldPartitioner;
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
import org.kitesdk.data.spi.TemporaryDatasetRepository;
import org.kitesdk.data.spi.TemporaryDatasetRepositoryAccessor;
import org.kitesdk.data.spi.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * A {@link org.kitesdk.data.spi.DatasetRepository} that stores data in a Hadoop {@link FileSystem}.
 * </p>
 * <p>
 * Given a {@link FileSystem}, a root directory, and a {@link MetadataProvider},
 * this {@link org.kitesdk.data.spi.DatasetRepository} implementation can load and store
 * {@link Dataset}s on both local filesystems as well as the Hadoop Distributed
 * FileSystem (HDFS). Users may directly instantiate this class with the three
 * dependencies above and then perform dataset-related operations using any of
 * the provided methods. The primary methods of interest will be
 * {@link #create(String, String, DatasetDescriptor)},
 * {@link #load(String, String)}, and
 * {@link #delete(String, String)} which create a new dataset, load an existing
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
 * @see org.kitesdk.data.Dataset
 * @see org.kitesdk.data.DatasetDescriptor
 * @see org.kitesdk.data.PartitionStrategy
 * @see org.kitesdk.data.spi.DatasetRepository
 * @see org.kitesdk.data.spi.MetadataProvider
 */
public class FileSystemDatasetRepository extends AbstractDatasetRepository
    implements TemporaryDatasetRepositoryAccessor {

  private static final Logger LOG = LoggerFactory
    .getLogger(FileSystemDatasetRepository.class);

  private final MetadataProvider metadataProvider;
  private final Configuration conf;
  private final FileSystem fs;
  private final Path rootDirectory;
  private final URI repositoryUri;

  public FileSystemDatasetRepository(Configuration conf, Path rootDirectory) {
    this(conf, rootDirectory, new FileSystemMetadataProvider(conf, rootDirectory));
  }

  public FileSystemDatasetRepository(
      Configuration conf, Path rootDirectory, MetadataProvider provider) {
    Preconditions.checkNotNull(conf, "Configuration cannot be null");
    Preconditions.checkNotNull(rootDirectory, "Root directory cannot be null");
    Preconditions.checkNotNull(provider, "Metadata provider cannot be null");

    try {
      this.fs = rootDirectory.getFileSystem(conf);
    } catch (IOException e) {
      throw new DatasetIOException(
          "Cannot get FileSystem for repository location: " + rootDirectory, e);
    }

    this.conf = conf;
    this.rootDirectory = fs.makeQualified(rootDirectory);
    this.repositoryUri = URI.create("repo:" + this.rootDirectory.toUri());
    this.metadataProvider = provider;
  }

  @Override
  public <E> Dataset<E> create(String namespace, String name,
                               DatasetDescriptor descriptor, Class<E> type) {
    Preconditions.checkNotNull(namespace, "Namespace cannot be null");
    Preconditions.checkNotNull(name, "Dataset name cannot be null");
    Preconditions.checkNotNull(descriptor, "Descriptor cannot be null");

    // suggest a location for this dataset: <root>/<namespace>/<name>/
    Path suggestedLocation = pathForDataset(namespace, name);

    DatasetDescriptor newDescriptor = descriptor;
    if (descriptor.getLocation() == null) {
      newDescriptor = new DatasetDescriptor.Builder(descriptor)
          .location(suggestedLocation) // may be overridden by MetadataProvider
          .build();
    }

    newDescriptor = metadataProvider.create(namespace, name, newDescriptor);

    FileSystemUtil.ensureLocationExists(newDescriptor, conf);

    LOG.debug("Created dataset: {} schema: {} datasetPath: {}", new Object[] {
        name, newDescriptor.getSchema(), newDescriptor.getLocation() });

    return new FileSystemDataset.Builder<E>()
        .namespace(namespace)
        .name(name)
        .configuration(conf)
        .descriptor(newDescriptor)
        .type(type)
        .uri(new URIBuilder(getUri(), namespace, name).build())
        .partitionKey(newDescriptor.isPartitioned() ? new PartitionKey() : null)
        .partitionListener(getPartitionListener())
        .build();
  }

  @Override
  public <E> Dataset<E> update(String namespace, String name,
                               DatasetDescriptor descriptor, Class<E> type) {
    Preconditions.checkNotNull(namespace, "Namespace cannot be null");
    Preconditions.checkNotNull(name, "Dataset name cannot be null");
    Preconditions.checkNotNull(descriptor, "Descriptor cannot be null");

    DatasetDescriptor oldDescriptor = metadataProvider.load(namespace, name);

    // oldDescriptor is valid if load didn't throw NoSuchDatasetException
    Compatibility.checkUpdate(oldDescriptor, descriptor);

    DatasetDescriptor updatedDescriptor = metadataProvider.update(namespace, name, descriptor);

    LOG.debug("Updated dataset: {} schema: {} location: {}", new Object[] {
        name, updatedDescriptor.getSchema(), updatedDescriptor.getLocation() });

    return new FileSystemDataset.Builder<E>()
        .namespace(namespace)
        .name(name)
        .configuration(conf)
        .descriptor(updatedDescriptor)
        .type(type)
        .uri(new URIBuilder(getUri(), namespace, name).build())
        .partitionKey(updatedDescriptor.isPartitioned() ? new PartitionKey() : null)
        .partitionListener(getPartitionListener())
        .build();
  }

  @Override
  public <E> Dataset<E> load(String namespace, String name, Class<E> type) {
    Preconditions.checkNotNull(namespace, "Namespace cannot be null");
    Preconditions.checkNotNull(name, "Dataset name cannot be null");

    LOG.debug("Loading dataset: {}", name);

    DatasetDescriptor descriptor = metadataProvider.load(namespace, name);

    FileSystemDataset<E> ds = new FileSystemDataset.Builder<E>()
        .namespace(namespace)
        .name(name)
        .configuration(conf)
        .descriptor(descriptor)
        .type(type)
        .uri(new URIBuilder(getUri(), namespace, name).build())
        .partitionKey(descriptor.isPartitioned() ? new PartitionKey() : null)
        .partitionListener(getPartitionListener())
        .build();

    LOG.debug("Loaded dataset:{}", ds);

    return ds;
  }

  @Override
  public boolean delete(String namespace, String name) {
    Preconditions.checkNotNull(namespace, "Namespace cannot be null");
    Preconditions.checkNotNull(name, "Dataset name cannot be null");

    LOG.debug("Deleting dataset:{}", name);

    DatasetDescriptor descriptor;
    try {
      descriptor = metadataProvider.load(namespace, name);
    } catch (DatasetNotFoundException ex) {
      return false;
    }

    // don't care about the return value here -- if it already doesn't exist
    // we still need to delete the data directory
    boolean changed = metadataProvider.delete(namespace, name);

    Path dataLocation = new Path(descriptor.getLocation());
    FileSystem dataFS = fsForPath(dataLocation, conf);

    if (fs.getUri().equals(dataFS.getUri())) {
      // the data location is on the right FS, so cleanlyDelete will work
      changed |= FileSystemUtil.cleanlyDelete(fs, rootDirectory, dataLocation);
    } else {
      try {
        if (dataFS.exists(dataLocation)) {
          if (dataFS.delete(dataLocation, true)) {
            changed = true;
          } else {
            throw new IOException(
                "Failed to delete dataset name:" + name +
                " location:" + dataLocation);
          }
        }
      } catch (IOException e) {
        throw new DatasetIOException(
            "Internal failure when removing location:" + dataLocation, e);
      }
    }
    return changed;
  }

  @Override
  public boolean exists(String namespace, String name) {
    Preconditions.checkNotNull(namespace, "Namespace cannot be null");
    Preconditions.checkNotNull(name, "Dataset name cannot be null");
    return metadataProvider.exists(namespace, name);
  }

  @Override
  public Collection<String> datasets(String namespace) {
    return metadataProvider.datasets(namespace);
  }

  @Override
  public Collection<String> namespaces() {
    return metadataProvider.namespaces();
  }

  @Override
  public URI getUri() {
    return repositoryUri;
  }

  @Override
  public TemporaryDatasetRepository getTemporaryRepository(String key) {
    return new TemporaryFileSystemDatasetRepository(conf, rootDirectory, key);
  }

  private Path pathForDataset(String namespace, String name) {
    return fs.makeQualified(pathForDataset(rootDirectory, namespace, name));
  }

  /**
   * Returns the correct dataset path for the given name and root directory.
   *
   * @param root A Path
   * @param name A String dataset name
   * @return the correct dataset Path
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE",
      justification="Checked in precondition")
  static Path pathForDataset(Path root, @Nullable String namespace, @Nullable String name) {
    Preconditions.checkNotNull(namespace, "Namespace cannot be null");
    Preconditions.checkNotNull(name, "Dataset name cannot be null");

    // Why replace '.' here? Is this a namespacing hack?
    return new Path(root, new Path(namespace, name.replace('.', Path.SEPARATOR_CHAR)));
  }

  /**
   * Get a {@link org.kitesdk.data.spi.PartitionKey} corresponding to a partition's filesystem path
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

      values.add(PathConversion.valueForDirname(fp, schema, stringValue));
    }
    return new PartitionKey(values.toArray(new Object[values.size()]));
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("fs", fs)
        .add("storage", rootDirectory)
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
      throw new DatasetIOException(
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
      if (configuration == null) {
        this.configuration = new Configuration();
      }

      Preconditions.checkState(this.rootDirectory != null,
          "No root directory defined");

      // the rootDirectory can have a scheme/authority that overrides

      if (fileSystem != null) {
        // if the FS doesn't match, this will throw IllegalArgumentException
        this.rootDirectory = fileSystem.makeQualified(rootDirectory);
      }

      return new FileSystemDatasetRepository(configuration, rootDirectory);
    }
  }
}
