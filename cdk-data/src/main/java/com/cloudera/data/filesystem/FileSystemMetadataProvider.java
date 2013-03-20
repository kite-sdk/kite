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
package com.cloudera.data.filesystem;

import com.cloudera.data.DatasetDescriptor;
import com.cloudera.data.MetadataProvider;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * A {@link MetadataProvider} that stores dataset metadata in a Hadoop
 * {@link FileSystem}.
 * </p>
 * <p>
 * When configured with a root directory, this implementation serializes the
 * information within a {@link DatasetDescriptor} on the provided
 * {@link FileSystem}. The descriptor is serialized as an Avro object and stored
 * in a directory named after the dataset name. For example, if the dataset name
 * is {@code logs}, the directory {@code rootDirectory/logs/} will be created,
 * if it doesn't exist, and the serialized descriptor will be stored in the file
 * {@code descriptor.avro}.
 * </p>
 */
public class FileSystemMetadataProvider implements MetadataProvider {

  private static final Logger logger = LoggerFactory
      .getLogger(FileSystemMetadataProvider.class);

  private static final String METADATA_DIRECTORY = ".metadata";
  private static final String SCHEMA_FILE_NAME = "schema.avsc";
  private static final String DESCRIPTOR_FILE_NAME = "descriptor.properties";
  private static final String PARTITION_EXPRESSION_FIELD_NAME = "partitionExpression";
  private static final String VERSION_FIELD_NAME = "version";
  private static final String METADATA_VERSION = "1";

  private final Path rootDirectory;
  private final FileSystem fileSystem;

  public FileSystemMetadataProvider(FileSystem fileSystem, Path rootDirectory) {
    this.fileSystem = fileSystem;
    this.rootDirectory = rootDirectory;
  }

  @Override
  public DatasetDescriptor load(String name) throws IOException {
    logger.debug("Loading dataset metadata name:{}", name);

    Path directory = new Path(pathForDataset(name), METADATA_DIRECTORY);

    InputStream inputStream = null;
    Properties properties = new Properties();
    DatasetDescriptor.Builder builder = new DatasetDescriptor.Builder();

    try {
      inputStream = fileSystem.open(new Path(directory, DESCRIPTOR_FILE_NAME));
      properties.load(inputStream);

      if (properties.containsKey(PARTITION_EXPRESSION_FIELD_NAME)) {
        builder.partitionStrategy(new PartitionExpression(properties
            .getProperty(PARTITION_EXPRESSION_FIELD_NAME), true).evaluate());
      }
    } finally {
      Closeables.closeQuietly(inputStream);
    }

    try {
      inputStream = fileSystem.open(new Path(directory, SCHEMA_FILE_NAME));
      builder.schema(new Schema.Parser().parse(new String(ByteStreams
          .toByteArray(inputStream), Charsets.UTF_8)));

    } finally {
      Closeables.closeQuietly(inputStream);
    }

    return builder.get();
  }

  @Override
  public void save(String name, DatasetDescriptor descriptor)
      throws IOException {

    logger.debug("Saving dataset metadata name:{} descriptor:{}", name,
        descriptor);

    FSDataOutputStream outputStream = null;
    Path directory = new Path(pathForDataset(name), METADATA_DIRECTORY);

    if (!fileSystem.exists(directory)) {
      fileSystem.mkdirs(directory);
    }

    try {
      outputStream = fileSystem.create(new Path(directory, SCHEMA_FILE_NAME));
      outputStream.write(descriptor.getSchema().toString(true)
          .getBytes(Charsets.UTF_8));
      outputStream.flush();
    } finally {
      Closeables.closeQuietly(outputStream);
    }

    Properties properties = new Properties();
    properties.setProperty(VERSION_FIELD_NAME, METADATA_VERSION);

    if (descriptor.isPartitioned()) {
      properties.setProperty(PARTITION_EXPRESSION_FIELD_NAME,
          PartitionExpression.toExpression(descriptor.getPartitionStrategy()));
    }

    try {
      outputStream = fileSystem
          .create(new Path(directory, DESCRIPTOR_FILE_NAME));
      properties.store(outputStream, "Dataset descriptor for " + name);
      outputStream.flush();
    } finally {
      Closeables.closeQuietly(outputStream);
    }
  }

  @Override
  public boolean delete(String name) throws IOException {
    logger.debug("Deleting dataset metadata name:{}", name);

    Path directory = new Path(pathForDataset(name), METADATA_DIRECTORY);

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
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("rootDirectory", rootDirectory)
        .add("fileSystem", fileSystem).toString();
  }

  private Path pathForDataset(String name) {
    Preconditions.checkState(rootDirectory != null,
        "Dataset repository root directory can not be null");

    /*
     * I'm pretty sure that HDFS doesn't use platform-specific path separators.
     * What does it use on Windows?
     */
    return new Path(rootDirectory, name.replace('.', '/'));
  }

}
