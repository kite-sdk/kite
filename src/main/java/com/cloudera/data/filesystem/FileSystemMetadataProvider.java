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
import com.cloudera.data.PartitionExpression;
import com.cloudera.data.PartitionStrategy;
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.google.common.io.Resources;

public class FileSystemMetadataProvider implements MetadataProvider {

  private static final Logger logger = LoggerFactory
      .getLogger(FileSystemMetadataProvider.class);

  private static final String schemaPath = "DatasetDescriptor.avsc";

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
          "descriptor.avro")), 0);
      reader = new DataFileReader<Record>(inputStream,
          new GenericDatumReader<Record>(descriptorSchema));

      Record record = reader.next();

      datasetDescriptor = new DatasetDescriptor.Builder()
          .schema(
              new Schema.Parser().parse(((Utf8) record.get("schema"))
                  .toString()))
          .partitionStrategy(
              record.get("partitionExpression") != null ? new PartitionExpression(
                  ((Utf8) record.get("partitionExpression")).toString(), true)
                  .evaluate() : null).get();

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

    Schema schema = descriptor.getSchema();
    PartitionStrategy partitionStrategy = descriptor.getPartitionStrategy();

    FSDataOutputStream outputStream = null;
    Path directory = pathForDataset(name);
    DataFileWriter<Record> writer = new DataFileWriter<Record>(
        new GenericDatumWriter<Record>(descriptorSchema));

    try {
      outputStream = fileSystem.create(new Path(directory, "descriptor.avro"));
      writer.create(descriptorSchema, outputStream);

      writer.append(new GenericRecordBuilder(descriptorSchema)
          .set("schema", schema.toString())
          .set(
              "partitionExpression",
              partitionStrategy != null ? PartitionExpression
                  .toExpression(partitionStrategy) : null).build());

      writer.flush();
    } finally {
      Closeables.closeQuietly(outputStream);
      Closeables.closeQuietly(writer);
    }
  }

  private void ensureDescriptorSchema() throws IOException {
    if (descriptorSchema == null) {
      descriptorSchema = new Schema.Parser().parse(Resources.getResource(
          schemaPath).openStream());
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
