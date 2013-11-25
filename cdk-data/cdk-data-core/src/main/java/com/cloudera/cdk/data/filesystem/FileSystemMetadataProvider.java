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
import com.cloudera.cdk.data.MetadataProvider;
import com.cloudera.cdk.data.MetadataProviderException;
import com.cloudera.cdk.data.impl.Accessor;
import com.cloudera.cdk.data.spi.AbstractMetadataProvider;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import java.io.FileNotFoundException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

/**
 * <p>
 * A {@link MetadataProvider} that stores dataset metadata in a Hadoop
 * {@link FileSystem}.
 * </p>
 * <p>
 * When configured with a root directory, this implementation serializes the
 * information within a {@link com.cloudera.cdk.data.DatasetDescriptor} on the provided
 * {@link FileSystem}. The descriptor is serialized as an Avro object and stored
 * in a directory named after the dataset name. For example, if the dataset name
 * is {@code logs}, the directory {@code rootDirectory/logs/} will be created,
 * if it doesn't exist, and the serialized descriptor will be stored in the file
 * {@code descriptor.avro}.
 * </p>
 */
public class FileSystemMetadataProvider extends AbstractMetadataProvider {

  private static final Logger logger = LoggerFactory
    .getLogger(FileSystemMetadataProvider.class);

  private static final String METADATA_DIRECTORY = ".metadata";
  private static final String SCHEMA_FILE_NAME = "schema.avsc";
  private static final String DESCRIPTOR_FILE_NAME = "descriptor.properties";
  private static final String PARTITION_EXPRESSION_FIELD_NAME = "partitionExpression";
  private static final String VERSION_FIELD_NAME = "version";
  private static final String METADATA_VERSION = "1";
  private static final String FORMAT_FIELD_NAME = "format";
  private static final String LOCATION_FIELD_NAME = "location";

  private static final Set<String> RESERVED_PROPERTIES = Sets.newHashSet(
      PARTITION_EXPRESSION_FIELD_NAME, VERSION_FIELD_NAME, FORMAT_FIELD_NAME,
      LOCATION_FIELD_NAME);

  private final Configuration conf;
  private final Path rootDirectory;

  // cache the rootDirectory's FileSystem to avoid multiple lookups
  private transient final FileSystem rootFileSystem;

  /**
   * All metadata is stored under rootDirectory. Data may also be stored under
   * rootDirectory if no location is set on incoming descriptors.
   *
   * @deprecated will be removed in 0.9.0
   */
  @Deprecated
  public FileSystemMetadataProvider(FileSystem fileSystem, Path rootDirectory) {
    Preconditions.checkArgument(fileSystem != null,
        "FileSystem cannot be null");
    Preconditions.checkArgument(rootDirectory != null, "Root cannot be null");

    this.conf = new Configuration();
    // the default FS should be the one given
    this.conf.set("fs.defaultFS", fileSystem.getUri().toString());
    this.rootDirectory = fileSystem.makeQualified(rootDirectory);
    try {
      // get the FS for the root, in case they don't match
      this.rootFileSystem = rootDirectory.getFileSystem(conf);
    } catch (IOException ex) {
      throw new MetadataProviderException(
          "Cannot get FileSystem for root path", ex);
    }
  }

  /**
   * @deprecated will be removed in 0.9.0
   */
  @Deprecated
  public FileSystemMetadataProvider(Configuration conf, Path rootDirectory) {
    Preconditions.checkArgument(conf != null, "Configuration cannot be null");
    Preconditions.checkArgument(rootDirectory != null, "Root cannot be null");

    this.conf = conf;
    try {
      this.rootFileSystem = rootDirectory.getFileSystem(conf);
      this.rootDirectory = rootFileSystem.makeQualified(rootDirectory);
    } catch (IOException ex) {
      throw new MetadataProviderException(
          "Cannot get FileSystem for root path", ex);
    }
  }

  @Override
  public DatasetDescriptor load(String name) {
    Preconditions.checkArgument(name != null, "Name cannot be null");

    logger.debug("Loading dataset metadata name:{}", name);

    final Path metadataPath = pathForMetadata(name);
    checkExists(rootFileSystem, metadataPath);

    InputStream inputStream = null;
    Properties properties = new Properties();
    DatasetDescriptor.Builder builder = new DatasetDescriptor.Builder();
    Path descriptorPath = new Path(metadataPath, DESCRIPTOR_FILE_NAME);

    boolean threw = true;
    try {
      inputStream = rootFileSystem.open(descriptorPath);
      properties.load(inputStream);
      threw = false;
    } catch (IOException e) {
      throw new MetadataProviderException(
          "Unable to load descriptor file:" + descriptorPath + " for dataset:" + name, e);
    } finally {
      try {
        Closeables.close(inputStream, threw);
      } catch (IOException e) {
        throw new MetadataProviderException(e);
      }
    }

    if (properties.containsKey(FORMAT_FIELD_NAME)) {
      builder.format(Accessor.getDefault().newFormat(
          properties.getProperty(FORMAT_FIELD_NAME)));
    }
    if (properties.containsKey(PARTITION_EXPRESSION_FIELD_NAME)) {
      builder.partitionStrategy(Accessor.getDefault().fromExpression(properties
          .getProperty(PARTITION_EXPRESSION_FIELD_NAME)));
    }
    Path schemaPath = new Path(metadataPath, SCHEMA_FILE_NAME);
    try {
      builder.schemaUri(rootFileSystem.makeQualified(schemaPath).toUri());
    } catch (IOException e) {
      throw new MetadataProviderException(
        "Unable to load schema file:" + schemaPath + " for dataset:" + name, e);
    }

    final Path location;
    if (properties.containsKey(LOCATION_FIELD_NAME)) {
      // the location should always be written by this library and validated
      // when the descriptor is first created.
      location = new Path(properties.getProperty(LOCATION_FIELD_NAME));
    } else {
      // backwards-compatibility: older versions didn't write this property
      location = pathForDataset(name);
    }
    builder.location(location);

    // custom properties
    for (String property : properties.stringPropertyNames()) {
      if (!RESERVED_PROPERTIES.contains(property)) {
        builder.property(property, properties.getProperty(property));
      }
    }

    return builder.build();
  }

  @Override
  public DatasetDescriptor create(String name, DatasetDescriptor descriptor) {
    Preconditions.checkArgument(name != null, "Name cannot be null");
    Preconditions.checkArgument(descriptor != null,
        "Descriptor cannot be null");

    logger.debug("Saving dataset metadata name:{} descriptor:{}", name,
        descriptor);

    final Path dataLocation;

    // If the descriptor has a location, use it.
    if (descriptor.getLocation() != null) {
      dataLocation = new Path(descriptor.getLocation());
    } else {
      dataLocation = pathForDataset(name);
    }

    final Path metadataLocation = pathForMetadata(name);

    // get a DatasetDescriptor with the location set
    DatasetDescriptor newDescriptor = new DatasetDescriptor.Builder(descriptor)
        .location(dataLocation)
        .build();

    try {
      if (rootFileSystem.exists(metadataLocation)) {
        throw new DatasetExistsException(
            "Descriptor directory:" + metadataLocation + " already exists");
      }
      // create the directory so that update can do the rest of the work
      rootFileSystem.mkdirs(metadataLocation);
    } catch (IOException e) {
      throw new MetadataProviderException(
          "Unable to create metadata directory:" + metadataLocation +
          " for dataset:" + name, e);
    }

    writeDescriptor(rootFileSystem, metadataLocation, name, newDescriptor);

    return newDescriptor;
  }

  @Override
  public DatasetDescriptor update(String name, DatasetDescriptor descriptor) {
    Preconditions.checkArgument(name != null, "Name cannot be null");
    Preconditions.checkArgument(descriptor != null,
        "Descriptor cannot be null");

    logger.debug("Saving dataset metadata name:{} descriptor:{}", name,
      descriptor);

    writeDescriptor(
        rootFileSystem, pathForMetadata(name), name, descriptor);

    return descriptor;
  }

  @Override
  public boolean delete(String name) {
    Preconditions.checkArgument(name != null, "Name cannot be null");

    logger.debug("Deleting dataset metadata name:{}", name);

    final Path metadataDirectory = pathForMetadata(name);

    try {
      if (rootFileSystem.exists(metadataDirectory)) {
        if (rootFileSystem.delete(metadataDirectory, true)) {
          return true;
        } else {
          throw new IOException("Failed to delete metadata directory:"
            + metadataDirectory);
        }
      } else {
        return false;
      }
    } catch (IOException e) {
      throw new MetadataProviderException(
          "Unable to find or delete metadata directory:" + metadataDirectory +
          " for dataset:" + name, e);
    }
  }

  @Override
  public boolean exists(String name) {
    Preconditions.checkArgument(name != null, "Name cannot be null");

    final Path potentialPath = pathForMetadata(name);
    try {
      return rootFileSystem.exists(potentialPath);
    } catch (IOException ex) {
      throw new MetadataProviderException(
          "Could not check metadata path:" + potentialPath, ex);
    }
  }

  @Override
  public List<String> list() {
    List<String> datasets = Lists.newArrayList();
    try {
      FileStatus[] entries = rootFileSystem.listStatus(rootDirectory,
          PathFilters.notHidden());
      for (FileStatus entry : entries) {
        // assumes that all unhidden directories under the root are data sets
        if (entry.isDirectory() &&
            rootFileSystem.exists(new Path(entry.getPath(), ".metadata"))) {
          // may want to add a check: !RESERVED_NAMES.contains(name)
          datasets.add(entry.getPath().getName());
        } else {
          continue;
        }
      }
    } catch (FileNotFoundException ex) {
      // the repo hasn't created any files yet
      return datasets;
    } catch (IOException ex) {
      throw new MetadataProviderException("Could not list data sets", ex);
    }
    return datasets;
  }

  /**
   * Returns the root directory where metadata is stored.
   *
   * @return a Path where {@link DatasetDescriptor}s are stored
   *
   * @since 0.8.0
   */
  Path getRootDirectory() {
    return rootDirectory;
  }

  /**
   * Returns the file system where metadata is stored.
   *
   * @return a FileSystem
   *
   * @since 0.8.0
   */
  FileSystem getFileSytem() {
    return rootFileSystem;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("rootDirectory", rootDirectory)
        .add("conf", conf).toString();
  }

  private Path pathForDataset(String name) {
    Preconditions.checkState(rootDirectory != null,
      "Dataset repository root directory can not be null");

    return rootFileSystem.makeQualified(pathForDataset(rootDirectory, name));
  }

  private Path pathForMetadata(String name) {
    Preconditions.checkState(rootDirectory != null,
      "Dataset repository root directory can not be null");

    return pathForMetadata(rootDirectory, name);
  }

  /**
   * Writes the contents of a {@code Descriptor} to files.
   *
   * @param fs                The {@link FileSystem} where data will be stored
   * @param metadataLocation  The directory {@link Path} where metadata files
   *                          will be located
   * @param name              The {@link Dataset} name
   * @param descriptor        The {@code Descriptor} contents to write
   *
   * @throws MetadataProviderException  If the {@code metadataLocation} does not
   *                                    exist or if any IOExceptions need to be
   *                                    propagated.
   */
  private static void writeDescriptor(
      FileSystem fs, Path metadataLocation, String name,
      DatasetDescriptor descriptor) {

    checkExists(fs, metadataLocation);

    FSDataOutputStream outputStream = null;
    final Path schemaPath = new Path(metadataLocation, SCHEMA_FILE_NAME);
    boolean threw = true;
    try {
      outputStream = fs.create(schemaPath, true /* overwrite */ );
      outputStream.write(descriptor.getSchema().toString(true)
          .getBytes(Charsets.UTF_8));
      outputStream.flush();
      threw = false;
    } catch (IOException e) {
      throw new MetadataProviderException(
          "Unable to save schema file:" + schemaPath + " for dataset:" + name, e);
    } finally {
      try {
        Closeables.close(outputStream, threw);
      } catch (IOException e) {
        throw new MetadataProviderException(e);
      }
    }

    Properties properties = new Properties();
    properties.setProperty(VERSION_FIELD_NAME, METADATA_VERSION);
    properties.setProperty(FORMAT_FIELD_NAME, descriptor.getFormat().getName());

    final URI dataLocation = descriptor.getLocation();
    if (dataLocation != null) {
      properties.setProperty(LOCATION_FIELD_NAME, dataLocation.toString());
    }

    if (descriptor.isPartitioned()) {
      properties.setProperty(PARTITION_EXPRESSION_FIELD_NAME,
          Accessor.getDefault().toExpression(descriptor.getPartitionStrategy()));
    }

    // copy custom properties to the table
    for (String property : descriptor.listProperties()) {
      // no need to check the reserved list, those are not set on descriptors
      properties.setProperty(property, descriptor.getProperty(property));
    }

    final Path descriptorPath = new Path(metadataLocation, DESCRIPTOR_FILE_NAME);
    threw = true;
    try {
      outputStream = fs.create(descriptorPath, true /* overwrite */ );
      properties.store(outputStream, "Dataset descriptor for " + name);
      outputStream.flush();
      threw = false;
    } catch (IOException e) {
      throw new MetadataProviderException(
          "Unable to save descriptor file:" + descriptorPath + " for dataset:" + name, e);
    } finally {
      try {
        Closeables.close(outputStream, threw);
      } catch (IOException e) {
        throw new MetadataProviderException(e);
      }
    }
  }

  /**
   * Returns the correct metadata path for the given dataset.
   * @param root A Path
   * @param name A String dataset name
   * @return the metadata Path
   */
  private static Path pathForMetadata(Path root, String name) {
    return new Path(pathForDataset(root, name), METADATA_DIRECTORY);
  }

  /**
   * Returns the correct dataset path for the given name and root directory.
   *
   * @param root A Path
   * @param name A String dataset name
   * @return the correct dataset Path
   */
  private static Path pathForDataset(Path root, String name) {
    Preconditions.checkArgument(name != null, "Dataset name cannot be null");

    // Why replace '.' here? Is this a namespacing hack?
    return new Path(root, name.replace('.', Path.SEPARATOR_CHAR));
  }

  /**
   * Precondition-style static validation that a dataset exists
   *
   * @param fs        A FileSystem where the metadata should be stored
   * @param location  The Path where the metadata should be stored
   * @throws com.cloudera.cdk.data.NoSuchDatasetException if the descriptor location is missing
   * @throws MetadataProviderException  if any IOException is thrown
   */
  @SuppressWarnings("deprecation")
  private static void checkExists(FileSystem fs, Path location) {
    try {
      if (!fs.exists(location)) {
        throw new com.cloudera.cdk.data.NoSuchDatasetException("Descriptor location is missing: " +
            location);
      }
    } catch (IOException ex) {
      throw new MetadataProviderException("Cannot access descriptor location", ex);
    }
  }

  /**
   * A fluent builder to aid in the construction of {@link FileSystemMetadataProvider}
   * instances.
   * @since 0.8.0
   */
  public static class Builder implements Supplier<FileSystemMetadataProvider> {

    private Path rootDirectory;
    private Configuration configuration;

    /**
     * The root directory for metadata files.
     *
     * @param path a Path to a FileSystem location
     * @return this Builder for method chaining.
     */
    public Builder rootDirectory(Path path) {
      this.rootDirectory = path;
      return this;
    }

    /**
     * The {@link Configuration} used to find the {@link FileSystem}.
     */
    public Builder configuration(Configuration configuration) {
      this.configuration = configuration;
      return this;
    }

    /**
     * @deprecated will be removed in 0.10.0
     */
    @Override
    @Deprecated
    public FileSystemMetadataProvider get() {
      return build();
    }

    @SuppressWarnings("deprecation")
    public FileSystemMetadataProvider build() {
      return new FileSystemMetadataProvider(configuration, rootDirectory);
    }
  }

}
