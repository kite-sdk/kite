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

import com.google.common.annotations.VisibleForTesting;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetExistsException;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.impl.Accessor;
import org.kitesdk.data.spi.AbstractDatasetRepository;
import org.kitesdk.data.spi.AbstractMetadataProvider;
import org.kitesdk.data.spi.MetadataProvider;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import java.io.FileNotFoundException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.spi.Compatibility;
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
 * information within a {@link org.kitesdk.data.DatasetDescriptor} on the provided
 * {@link FileSystem}. The descriptor is serialized as an Avro object and stored
 * in a directory named after the dataset name. For example, if the dataset name
 * is {@code logs}, the directory {@code rootDirectory/logs/} will be created,
 * if it doesn't exist, and the serialized descriptor will be stored in the file
 * {@code descriptor.avro}.
 * </p>
 */
public class FileSystemMetadataProvider extends AbstractMetadataProvider {

  private static final Logger LOG = LoggerFactory
    .getLogger(FileSystemMetadataProvider.class);

  private static final String METADATA_DIRECTORY = ".metadata";
  private static final String SCHEMA_FILE_NAME = "schema.avsc";
  private static final String SCHEMA_DIRECTORY_NAME = "schemas";
  private static final String DESCRIPTOR_FILE_NAME = "descriptor.properties";
  private static final String PARTITION_EXPRESSION_FIELD_NAME = "partitionExpression";
  private static final String VERSION_FIELD_NAME = "version";
  private static final String METADATA_VERSION = "1";
  private static final String FORMAT_FIELD_NAME = "format";
  private static final String LOCATION_FIELD_NAME = "location";
  private static final String COMPRESSION_TYPE_FIELD_NAME = "compressionType";
  private static final String DEFAULT_NAMESPACE = "default";

  private static final Set<String> RESERVED_PROPERTIES = Sets.newHashSet(
      PARTITION_EXPRESSION_FIELD_NAME, VERSION_FIELD_NAME, FORMAT_FIELD_NAME,
      LOCATION_FIELD_NAME, COMPRESSION_TYPE_FIELD_NAME);

  private final Configuration conf;
  private final Path rootDirectory;

  // cache the rootDirectory's FileSystem to avoid multiple lookups
  private transient final FileSystem rootFileSystem;

  public FileSystemMetadataProvider(Configuration conf, Path rootDirectory) {
    Preconditions.checkNotNull(conf, "Configuration cannot be null");
    Preconditions.checkNotNull(rootDirectory, "Root directory cannot be null");

    this.conf = conf;
    try {
      this.rootFileSystem = rootDirectory.getFileSystem(conf);
      this.rootDirectory = rootFileSystem.makeQualified(rootDirectory);
    } catch (IOException ex) {
      throw new DatasetIOException("Cannot get FileSystem for root path", ex);
    }
  }

  @Override
  public DatasetDescriptor load(String namespace, String name) {
    Preconditions.checkNotNull(namespace, "Namespace cannot be null");
    Preconditions.checkNotNull(name, "Dataset name cannot be null");

    LOG.debug("Loading dataset metadata name: {}", name);

    Path metadataPath = find(namespace, name);

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
      throw new DatasetIOException(
          "Unable to load descriptor file:" + descriptorPath +
          " for dataset:" + name, e);
    } finally {
      try {
        Closeables.close(inputStream, threw);
      } catch (IOException e) {
        throw new DatasetIOException("Cannot close", e);
      }
    }

    if (properties.containsKey(FORMAT_FIELD_NAME)) {
      builder.format(Accessor.getDefault().newFormat(
          properties.getProperty(FORMAT_FIELD_NAME)));
    }
    if (properties.containsKey(COMPRESSION_TYPE_FIELD_NAME)) {
      builder.compressionType(properties.getProperty(COMPRESSION_TYPE_FIELD_NAME));
    }
    if (properties.containsKey(PARTITION_EXPRESSION_FIELD_NAME)) {
      builder.partitionStrategy(Accessor.getDefault().fromExpression(properties
          .getProperty(PARTITION_EXPRESSION_FIELD_NAME)));
    }

    SchemaManager manager = SchemaManager.load(conf,
        new Path(metadataPath, SCHEMA_DIRECTORY_NAME));

    URI schemaURI;

    if (manager == null) {

      // If there is no schema manager directory we are working
      // with a dataset written with an older version. Therefore
      // we just use a reference to the existing schema.
      Path schemaPath = new Path(metadataPath, SCHEMA_FILE_NAME);

      checkExists(getFileSytem(), schemaPath);

      schemaURI = rootFileSystem.makeQualified(schemaPath).toUri();

    } else {
      schemaURI = manager.getNewestSchemaURI();
    }

    if (schemaURI == null) {
      throw new DatasetException("Maformed dataset metadata at " +
          metadataPath + ". No schemas are present.");
    }

    try {
      builder.schemaUri(schemaURI);
    } catch (IOException e) {
      throw new DatasetIOException(
              "Unable to load schema file:" + schemaURI + " for dataset:" + name, e);
    }

    final Path location;
    if (properties.containsKey(LOCATION_FIELD_NAME)) {
      // the location should always be written by this library and validated
      // when the descriptor is first created.
      location = new Path(properties.getProperty(LOCATION_FIELD_NAME));
    } else {
      // backwards-compatibility: older versions didn't write this property but
      // the data and metadata were always co-located.
      location = expectedPathForDataset(namespace, name);
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
  public DatasetDescriptor create(String namespace, String name, DatasetDescriptor descriptor) {
    Preconditions.checkNotNull(namespace, "Namespace cannot be null");
    Preconditions.checkNotNull(name, "Dataset name cannot be null");
    Preconditions.checkNotNull(descriptor, "Descriptor cannot be null");
    Compatibility.check(namespace, name, descriptor);

    LOG.debug("Saving dataset metadata name:{} descriptor:{}", name,
        descriptor);

    // no need to check backward-compatibility when creating new datasets
    Path metadataLocation = pathForMetadata(namespace, name);

    try {
      if (rootFileSystem.exists(metadataLocation)) {
        throw new DatasetExistsException(
            "Descriptor directory already exists: " + metadataLocation);
      }
      // create the directory so that update can do the rest of the work
      rootFileSystem.mkdirs(metadataLocation);
    } catch (IOException e) {
      throw new DatasetIOException(
          "Unable to create metadata directory: " + metadataLocation +
          " for dataset: " + name, e);
    }

    writeDescriptor(rootFileSystem, metadataLocation, name, descriptor);

    return descriptor;
  }

  @Override
  public DatasetDescriptor update(String namespace, String name, DatasetDescriptor descriptor) {
    Preconditions.checkNotNull(namespace, "Namespace cannot be null");
    Preconditions.checkNotNull(name, "Dataset name cannot be null");
    Preconditions.checkNotNull(descriptor, "Descriptor cannot be null");
    Compatibility.check(namespace, name, descriptor);

    LOG.debug("Saving dataset metadata name: {} descriptor: {}", name,
        descriptor);

    Path metadataPath = find(namespace, name);
    writeDescriptor(rootFileSystem, metadataPath, name, descriptor);

    return descriptor;
  }

  @Override
  public boolean delete(String namespace, String name) {
    Preconditions.checkNotNull(namespace, "Namespace cannot be null");
    Preconditions.checkNotNull(name, "Dataset name cannot be null");

    LOG.debug("Deleting dataset metadata name: {}", name);

    Path metadataDirectory;
    try {
      metadataDirectory = find(namespace, name);
    } catch (DatasetNotFoundException _) {
      return false;
    }

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
      throw new DatasetIOException(
          "Unable to find or delete metadata directory:" + metadataDirectory +
          " for dataset:" + name, e);
    }
  }

  @Override
  public boolean exists(String namespace, String name) {
    Preconditions.checkNotNull(namespace, "Namespace cannot be null");
    Preconditions.checkNotNull(name, "Dataset name cannot be null");

    try {
      find(namespace, name);
      return true;
    } catch (DatasetNotFoundException e) {
      return false;
    }
  }

  @SuppressWarnings("deprecation")
  @Override
  public Set<String> namespaces() {
    Set<String> namespaces = Sets.newHashSet();
    try {
      FileStatus[] entries = rootFileSystem.listStatus(rootDirectory,
          PathFilters.notHidden());
      for (FileStatus entry : entries) {
        if (entry.isDir()) {
          // may want to add a check: !RESERVED_NAMES.contains(name)
          if (isNamespace(entry.getPath())) {
            namespaces.add(entry.getPath().getName());

          } else if (isDataset(entry.getPath())) {
            // add the default namespace for datasets with no namespace
            namespaces.add(DEFAULT_NAMESPACE);
          }
        }
      }
    } catch (FileNotFoundException ex) {
      // the repo hasn't created any files yet
      return namespaces;
    } catch (IOException ex) {
      throw new DatasetIOException("Could not list namespaces", ex);
    }
    return namespaces;
  }

  @SuppressWarnings("deprecation")
  @Override
  public Set<String> datasets(String namespace) {
    Preconditions.checkNotNull(namespace, "Namespace cannot be null");

    Set<String> datasets = Sets.newHashSet();

    try {
      // if using the default namespace, add datasets with no namespace dir
      if (DEFAULT_NAMESPACE.equals(namespace)) {
        FileStatus[] directEntries = rootFileSystem.listStatus(
            rootDirectory,
            PathFilters.notHidden());
        for (FileStatus entry : directEntries) {
          if (entry.isDir() && isDataset(entry.getPath())) {
            // may want to add a check: !RESERVED_NAMES.contains(name)
            datasets.add(entry.getPath().getName());
          }
        }
      }
    } catch (FileNotFoundException e) {
      // if the root directory doesn't exist, then no namespace directories do
      return datasets;
    } catch (IOException ex) {
      throw new DatasetIOException("Could not list datasets", ex);
    }

    try {
      FileStatus[] entries = rootFileSystem.listStatus(
          new Path(rootDirectory, namespace),
          PathFilters.notHidden());
      for (FileStatus entry : entries) {
        if (entry.isDir() && isDataset(entry.getPath())) {
          // may want to add a check: !RESERVED_NAMES.contains(name)
          datasets.add(entry.getPath().getName());
        }
      }

    } catch (FileNotFoundException ex) {
      // the repo hasn't created any files yet
      return datasets;
    } catch (IOException ex) {
      throw new DatasetIOException("Could not list datasets", ex);
    }
    return datasets;
  }

  /**
   * Returns whether the given {@code Path} contains directories with
   * {@code Dataset} metadata.
   *
   * @param dir a Path to check
   * @return {@code true} if there is a direct sub-directory with metadata
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  private boolean isNamespace(Path dir) throws IOException {
    FileStatus[] stats = rootFileSystem.listStatus(dir, PathFilters.notHidden());
    for (FileStatus stat : stats) {
      if (stat.isDir() && isDataset(stat.getPath())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns whether the given {@code Path} contains {@code Dataset} metadata.
   *
   * @param dir a Path to check
   * @return {@code true} if there is a .metadata subdirectory
   * @throws IOException
   */
  private boolean isDataset(Path dir) throws IOException {
    return rootFileSystem.isDirectory(new Path(dir, METADATA_DIRECTORY));
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

  private Path expectedPathForDataset(String namespace, String name) {
    return rootFileSystem.makeQualified(
        FileSystemDatasetRepository.pathForDataset(rootDirectory, namespace, name));
  }

  /**
   * Returns the path where this MetadataProvider will store metadata.
   *
   * Note that this is not dependent on the actual storage location for the
   * dataset, although they are usually co-located. This provider must be able
   * to read metadata without a location for the Dataset when loading.
   *
   * @param name The {@link Dataset} name
   * @return The directory {@link Path} where metadata files will be located
   */
  private Path pathForMetadata(String namespace, String name) {
    return pathForMetadata(rootDirectory, namespace, name);
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
   * @throws org.kitesdk.data.DatasetIOException
   *                          If the {@code metadataLocation} does not exist or
   *                          if any IOExceptions need to be propagated.
   */
  @VisibleForTesting
  static void writeDescriptor(
      FileSystem fs, Path metadataLocation, String name,
      DatasetDescriptor descriptor) {

    checkExists(fs, metadataLocation);

    // write the schema to the previous file location so
    // it can be read by earlier versions of Kite
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
      throw new DatasetIOException(
          "Unable to save schema file: " + schemaPath +
          " for dataset: " + name, e);
    } finally {
      try {
        Closeables.close(outputStream, threw);
      } catch (IOException e) {
        throw new DatasetIOException("Cannot close", e);
      }
    }

    // use the SchemaManager for schema operations moving forward
    SchemaManager manager = SchemaManager.create(fs.getConf(),
        new Path(metadataLocation, SCHEMA_DIRECTORY_NAME));

    manager.writeSchema(descriptor.getSchema());

    Properties properties = new Properties();
    properties.setProperty(VERSION_FIELD_NAME, METADATA_VERSION);
    properties.setProperty(FORMAT_FIELD_NAME, descriptor.getFormat().getName());
    properties.setProperty(COMPRESSION_TYPE_FIELD_NAME, descriptor.getCompressionType().getName());

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
      throw new DatasetIOException(
          "Unable to save descriptor file: " + descriptorPath +
          " for dataset: " + name, e);
    } finally {
      try {
        Closeables.close(outputStream, threw);
      } catch (IOException e) {
        throw new DatasetIOException("Cannot close", e);
      }
    }
  }

  /**
   * Returns the correct metadata path for the given dataset.
   * @param root A Path
   * @param name A String dataset name
   * @return the metadata Path
   */
  private static Path pathForMetadata(Path root, String namespace, String name) {
    return new Path(
        FileSystemDatasetRepository.pathForDataset(root, namespace, name),
        METADATA_DIRECTORY);
  }

  /**
   * Precondition-style static validation that a dataset exists
   *
   * @param fs        A FileSystem where the metadata should be stored
   * @param location  The Path where the metadata should be stored
   * @throws org.kitesdk.data.DatasetNotFoundException if the descriptor location is missing
   * @throws org.kitesdk.data.DatasetIOException  if any IOException is thrown
   */
  private static void checkExists(FileSystem fs, Path location) {
    try {
      if (!fs.exists(location)) {
        throw new DatasetNotFoundException(
            "Descriptor location does not exist: " + location);
      }
    } catch (IOException ex) {
      throw new DatasetIOException(
          "Cannot access descriptor location: " + location, ex);
    }
  }

  /**
   * This method provides backward-compatibility for finding metadata.
   * <p>
   * This handles the case where an existing program is opening a
   * DatasetRepository by URI. For example, the DatasetSink and maven plugin do
   * this. In that case, the repository URI will directly contain a directory
   * named for the dataset with .metadata in it. This checks for the updated
   * scheme and falls back to the old scheme if the namespace is "default".
   *
   * @param namespace the requested namespace.
   * @param name the dataset name.
   * @return a Path to the correct metadata directory
   * @throws DatasetNotFoundException if neither location has metadata
   */
  private Path find(String namespace, String name) {
    Path expectedPath = pathForMetadata(namespace, name);
    if (DEFAULT_NAMESPACE.equals(namespace)) {
      // when using the default namespace, the namespace may not be in the path
      try {
        checkExists(rootFileSystem, expectedPath);
        return expectedPath;
      } catch (DatasetNotFoundException e) {
        try {
          Path backwardCompatiblePath = new Path(rootDirectory, new Path(
              name.replace('.', Path.SEPARATOR_CHAR), METADATA_DIRECTORY));
          checkExists(rootFileSystem, backwardCompatiblePath);
          return backwardCompatiblePath;
        } catch (DatasetNotFoundException _) {
          throw e; // throw the original
        }
      }

    } else {
      // no need to check other locations
      checkExists(rootFileSystem, expectedPath);
      return expectedPath;
    }
  }
}
