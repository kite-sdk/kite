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

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.data.DatasetDescriptor;
import com.cloudera.data.MetadataProvider;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.google.common.io.Resources;

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

  private static final String DESCRIPTOR_SCHEMA_FILE = "DatasetDescriptor.avsc";
  private static final String DESCRIPTOR_FILE = "descriptor.avro";
  private static final String SCHEMA_FIELD_NAME = "schema";
  private static final String PARTITION_EXPRESSION_FIELD_NAME = "partitionExpression";

  private Path rootDirectory;
  private FileSystem fileSystem;

  private Schema descriptorSchema;

  public FileSystemMetadataProvider(FileSystem fileSystem, Path rootDirectory) {
    this.fileSystem = fileSystem;
    this.rootDirectory = rootDirectory;
  }

  @Override
  public DatasetDescriptor load(String name) throws IOException {
    logger.debug("Loading dataset metadata name:{}", name);

    ensureDescriptorSchema();

    Path directory = pathForDataset(name);
    AvroFSInput inputStream = null;
    DataFileReader<Record> reader = null;
    DatasetDescriptor datasetDescriptor = null;

    try {
      inputStream = new AvroFSInput(fileSystem.open(new Path(directory,
          DESCRIPTOR_FILE)), 0);
      reader = new DataFileReader<Record>(inputStream,
          new GenericDatumReader<Record>(descriptorSchema));

      Record record = reader.next();

      Object expr = record.get(PARTITION_EXPRESSION_FIELD_NAME);
      datasetDescriptor = new DatasetDescriptor.Builder()
          .schema(
              new Schema.Parser().parse(((Utf8) record.get(SCHEMA_FIELD_NAME))
                  .toString()))
          .partitionStrategy(expr != null ?
              new PartitionExpression(((Utf8) expr).toString(), true).evaluate() : null)
          .get();

      logger.debug("loaded descriptor:{}", record);
    } finally {
      Closeables.closeQuietly(reader);
      Closeables.closeQuietly(inputStream);
    }

    return datasetDescriptor;
  }

  @Override
  public void save(String name, DatasetDescriptor descriptor)
      throws IOException {
    logger.debug("Saving dataset metadata name:{} descriptor:{}", name,
        descriptor);

    ensureDescriptorSchema();

    FSDataOutputStream outputStream = null;
    Path directory = pathForDataset(name);
    DataFileWriter<Record> writer = new DataFileWriter<Record>(
        new GenericDatumWriter<Record>(descriptorSchema));

    try {
      outputStream = fileSystem.create(new Path(directory, DESCRIPTOR_FILE));
      writer.create(descriptorSchema, outputStream);

      writer.append(new GenericRecordBuilder(descriptorSchema)
          .set(SCHEMA_FIELD_NAME, descriptor.getSchema().toString())
          .set(
              PARTITION_EXPRESSION_FIELD_NAME,
              descriptor.isPartitioned() ? PartitionExpression
                  .toExpression(descriptor.getPartitionStrategy()) : null)
          .build());

      writer.flush();
    } finally {
      Closeables.closeQuietly(outputStream);
      Closeables.closeQuietly(writer);
    }
  }

  @Override
  public boolean delete(String name) throws IOException {
    logger.debug("Deleting dataset metadata name:{}", name);

    Path directory = pathForDataset(name);
    Path descriptorPath = new Path(directory, DESCRIPTOR_FILE);

    if (fileSystem.exists(descriptorPath)) {
      if (fileSystem.delete(descriptorPath, false)) {
        return true;
      } else {
        throw new IOException("Failed to delete metadata descriptor:"
            + descriptorPath);
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

  private void ensureDescriptorSchema() throws IOException {
    if (descriptorSchema == null) {
      descriptorSchema = new Schema.Parser().parse(Resources.getResource(
          DESCRIPTOR_SCHEMA_FILE).openStream());
    }
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
