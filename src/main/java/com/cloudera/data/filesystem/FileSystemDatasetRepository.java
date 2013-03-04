package com.cloudera.data.filesystem;

import java.io.IOException;

import com.cloudera.data.impl.PartitionKey;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.data.Dataset;
import com.cloudera.data.DatasetDescriptor;
import com.cloudera.data.DatasetRepository;
import com.cloudera.data.MetadataProvider;
import com.cloudera.data.PartitionStrategy;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class FileSystemDatasetRepository implements DatasetRepository {

  private static final Logger logger = LoggerFactory
      .getLogger(FileSystemDatasetRepository.class);

  private MetadataProvider metadataProvider;

  private Path rootDirectory;
  private FileSystem fileSystem;

  public FileSystemDatasetRepository(FileSystem fileSystem, Path rootDirectory,
      MetadataProvider metadataProvider) {

    this.fileSystem = fileSystem;
    this.rootDirectory = rootDirectory;
    this.metadataProvider = metadataProvider;
  }

  @Deprecated
  public Dataset create(String name, Schema schema) throws IOException {
    Preconditions.checkArgument(name != null, "Name can not be null");
    Preconditions.checkArgument(schema != null, "Schema can not be null");
    Preconditions.checkState(fileSystem != null,
        "FileSystem implementation can not be null");

    Path datasetPath = pathForDataset(name);
    Path datasetDataPath = pathForDatasetData(name);

    if (fileSystem.exists(datasetPath)) {
      throw new IOException("Attempt to create an existing dataset:" + name);
    }

    logger.debug("Creating dataset:{} schema:{} datasetPath:{}", new Object[] {
        name, schema, datasetDataPath });

    if (!fileSystem.mkdirs(datasetDataPath)) {
      throw new IOException("Failed to make dataset diectories:"
          + datasetDataPath);
    }

    metadataProvider.save(name, new DatasetDescriptor.Builder().schema(schema)
        .get());

    FileSystemDataset.Builder datasetBuilder = new FileSystemDataset.Builder()
        .directory(datasetPath).dataDirectory(datasetDataPath)
        .fileSystem(fileSystem).name(name).schema(schema);

    String partitionExpression = schema.getProp("cdk.partition.expression");

    if (partitionExpression != null) {
      logger.debug("Partition expression: {}", partitionExpression);
      PartitionExpression expr = new PartitionExpression(partitionExpression,
          true);
      datasetBuilder.partitionStrategy(expr.evaluate());
    }

    return datasetBuilder.get();
  }

  @Override
  public Dataset create(String name, DatasetDescriptor descriptor)
      throws IOException {

    Preconditions.checkArgument(name != null, "Name can not be null");
    Preconditions.checkArgument(descriptor != null,
        "Descriptor can not be null");
    Preconditions.checkState(metadataProvider != null,
        "Metadata provider can not be null");

    Schema schema = descriptor.getSchema();
    PartitionStrategy partitionStrategy = descriptor.getPartitionStrategy();

    Path datasetPath = pathForDataset(name);
    Path datasetDataPath = pathForDatasetData(name);

    if (fileSystem.exists(datasetPath)) {
      throw new IOException("Attempt to create an existing dataset:" + name);
    }

    logger.debug("Creating dataset:{} schema:{} datasetPath:{}", new Object[] {
        name, schema, datasetDataPath });

    if (!fileSystem.mkdirs(datasetDataPath)) {
      throw new IOException("Failed to make dataset diectories:"
          + datasetDataPath);
    }

    metadataProvider.save(name, descriptor);

    return new FileSystemDataset.Builder().name(name).fileSystem(fileSystem)
        .schema(schema).directory(pathForDataset(name))
        .dataDirectory(pathForDatasetData(name))
        .partitionStrategy(partitionStrategy)
        .partitionKey(partitionStrategy == null ? null : new PartitionKey())
        .get();
  }

  @Override
  public Dataset get(String name) throws IOException {
    Preconditions.checkArgument(name != null, "Name can not be null");
    Preconditions.checkState(rootDirectory != null,
        "Root directory can not be null");
    Preconditions.checkState(fileSystem != null,
        "FileSystem implementation can not be null");
    Preconditions.checkState(metadataProvider != null,
        "Metadata provider can not be null");

    logger.debug("Loading dataset:{}", name);

    Path datasetDirectory = pathForDataset(name);
    Path datasetDataPath = pathForDatasetData(name);

    DatasetDescriptor descriptor = metadataProvider.load(name);
    Schema schema = descriptor.getSchema();
    PartitionStrategy partitionStrategy = descriptor.getPartitionStrategy();

    FileSystemDataset ds = new FileSystemDataset.Builder()
        .fileSystem(fileSystem).directory(datasetDirectory)
        .dataDirectory(datasetDataPath).name(name).schema(schema)
        .partitionStrategy(partitionStrategy).get();

    logger.debug("Loaded dataset:{}", ds);

    return ds;
  }

  private Path pathForDataset(String name) {
    Preconditions.checkState(rootDirectory != null,
        "Dataset repository root directory can not be null");

    return new Path(rootDirectory, name.replace('.', '/'));
  }

  private Path pathForDatasetData(String name) {
    return new Path(pathForDataset(name), "data");
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("rootDirectory", rootDirectory)
        .add("fileSystem", fileSystem).toString();
  }

  public Path getRootDirectory() {
    return rootDirectory;
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }

}
