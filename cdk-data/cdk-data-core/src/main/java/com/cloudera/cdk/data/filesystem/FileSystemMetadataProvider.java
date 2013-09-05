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
import com.cloudera.cdk.data.MetadataProvider;
import com.cloudera.cdk.data.MetadataProviderException;
import com.cloudera.cdk.data.NoSuchDatasetException;
import com.cloudera.cdk.data.impl.Accessor;
import com.cloudera.cdk.data.spi.AbstractMetadataProvider;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

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

  private final Path rootDirectory;
  private final FileSystem fileSystem;

  public FileSystemMetadataProvider(FileSystem fileSystem, Path rootDirectory) {
    this.fileSystem = fileSystem;
    this.rootDirectory = rootDirectory;
  }

  @Override
  public DatasetDescriptor load(String name) {
    logger.debug("Loading dataset metadata name:{}", name);

    final Path datasetPath = pathForDataset(name);
    FileSystemDatasetRepository.checkExists(fileSystem, datasetPath);

    final Path directory = pathForMetadata(datasetPath);
    checkExists(fileSystem, directory);

    InputStream inputStream = null;
    Properties properties = new Properties();
    DatasetDescriptor.Builder builder = new DatasetDescriptor.Builder();
    Path descriptorPath = new Path(directory, DESCRIPTOR_FILE_NAME);

    boolean threw = true;
    try {
      inputStream = fileSystem.open(descriptorPath);
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

    Path schemaPath = new Path(directory, SCHEMA_FILE_NAME);
    try {
      builder.schema(fileSystem.makeQualified(schemaPath).toUri());
    } catch (IOException e) {
      throw new MetadataProviderException(
        "Unable to load schema file:" + schemaPath + " for dataset:" + name, e);
    }

    return builder.get();
  }

  @Override
  public DatasetDescriptor create(String name, DatasetDescriptor descriptor) {
    logger.debug("Saving dataset metadata name:{} descriptor:{}", name,
      descriptor);

    final Path directory = pathForMetadata(pathForDataset(name));

    try {
      if (fileSystem.exists(directory)) {
        // TODO: Change this to a better Exception class
        throw new MetadataProviderException(
            "Descriptor directory:" + directory + "already exists");
      }
      // create the directory so that update can do the rest of the work
      fileSystem.mkdirs(directory);
    } catch (IOException e) {
      throw new MetadataProviderException(
        "Unable to create metadata directory:" + directory + " for dataset:" + name, e);
    }

    writeDescriptor(fileSystem, directory, name, descriptor);

    return descriptor;
  }

  @Override
  public DatasetDescriptor update(String name, DatasetDescriptor descriptor) {
    logger.debug("Saving dataset metadata name:{} descriptor:{}", name,
      descriptor);

    writeDescriptor(
        fileSystem, pathForMetadata(pathForDataset(name)), name, descriptor);

    return descriptor;
  }

  @Override
  public boolean delete(String name) {
    logger.debug("Deleting dataset metadata name:{}", name);

    final Path directory = pathForMetadata(pathForDataset(name));

    try {
      if (fileSystem.exists(directory)) {
        if (fileSystem.delete(directory, true)) {
          return true;
        } else {
          throw new IOException("Failed to delete metadata directory:"
            + directory);
        }
      } else {
        return false;
      }
    } catch (IOException e) {
      throw new MetadataProviderException(
        "Unable to find or delete metadata directory:" + directory + " for dataset:" + name, e);
    }
  }

  @Override
  public boolean exists(String name) {
    final Path potentialPath = pathForMetadata(pathForDataset(name));
    try {
      return fileSystem.exists(potentialPath);
    } catch (IOException ex) {
      throw new MetadataProviderException(
          "Could not check metadata path:" + potentialPath, ex);
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("rootDirectory", rootDirectory)
      .add("fileSystem", fileSystem).toString();
  }

  private Path pathForDataset(String name) {
    Preconditions.checkState(rootDirectory != null,
      "Dataset repository root directory can not be null");

    // delegate to the FileSystemDatasetRepository implementation
    return FileSystemDatasetRepository.pathForDataset(rootDirectory, name);
  }

  /**
   * Writes the contents of a {@code Descriptor} to files.
   *
   * @param fs          The {@link FileSystem} where data will be stored
   * @param location    The directory {@link Path} where files will be located
   * @param name        The {@link Dataset} name
   * @param descriptor  The {@code Descriptor} contents to write
   *
   * @throws MetadataProviderException If the location does not exist or if any
   *                                   IOExceptions need to be propagated.
   */
  private static void writeDescriptor(
      FileSystem fs, Path location, String name, DatasetDescriptor descriptor) {

    checkExists(fs, location);

    FSDataOutputStream outputStream = null;
    final Path schemaPath = new Path(location, SCHEMA_FILE_NAME);
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

    if (descriptor.isPartitioned()) {
      properties.setProperty(PARTITION_EXPRESSION_FIELD_NAME,
          Accessor.getDefault().toExpression(descriptor.getPartitionStrategy()));
    }

    final Path descriptorPath = new Path(location, DESCRIPTOR_FILE_NAME);
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
   * @param datasetPath the Dataset Path
   * @return the metadata Path
   */
  private static Path pathForMetadata(Path datasetPath) {
    return new Path(datasetPath, METADATA_DIRECTORY);
  }

  /**
   * Precondition-style static validation that a dataset exists
   *
   * @param fs        A FileSystem where the metadata should be stored
   * @param location  The Path where the metadata should be stored
   * @throws NoSuchDatasetException     if the descriptor location is missing
   * @throws MetadataProviderException  if any IOException is thrown
   * @since 0.7.0
   */
  protected static void checkExists(FileSystem fs, Path location) {
    try {
      if (!fs.exists(location)) {
        throw new NoSuchDatasetException("Descriptor location is missing");
      }
    } catch (IOException ex) {
      throw new MetadataProviderException("Cannot access descriptor location", ex);
    }
  }

}
