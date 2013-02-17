package com.cloudera.data.hdfs;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.data.DatasetRepository;
import com.cloudera.data.PartitionExpression;
import com.cloudera.data.hdfs.util.Paths;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;

public class HDFSDatasetRepository implements DatasetRepository<HDFSDataset> {

  private static final Logger logger = LoggerFactory
      .getLogger(HDFSDatasetRepository.class);

  private Path rootDirectory;
  private FileSystem fileSystem;

  public HDFSDatasetRepository(FileSystem fileSystem, Path rootDirectory) {
    this.fileSystem = fileSystem;
    this.rootDirectory = rootDirectory;
  }

  @Override
  public HDFSDataset create(String name, Schema schema) throws IOException {
    Preconditions.checkArgument(name != null, "Dataset name can not be null");
    Preconditions.checkArgument(schema != null,
        "Dataset schema can not be null");
    Preconditions.checkState(fileSystem != null,
        "Dataset repository filesystem implementation can not be null");

    Path datasetPath = pathForDataset(name);

    if (fileSystem.exists(datasetPath)) {
      throw new IOException("Attempt to create an existing dataset:" + name);
    }

    Path datasetDataPath = pathForDatasetData(name);
    Path datasetMetadataPath = pathForDatasetMetadata(name);

    logger.debug("Creating dataset:{} schema:{} datasetPath:{}", new Object[] {
        name, schema, datasetDataPath });

    if (!fileSystem.mkdirs(datasetDataPath)) {
      throw new IOException("Failed to make dataset diectories:"
          + datasetDataPath);
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Serializing dataset schema:{}", schema.toString());
    }

    Files.write(schema.toString(), Paths.toFile(datasetMetadataPath),
        Charsets.UTF_8);

    HDFSDataset.Builder datasetBuilder = new HDFSDataset.Builder()
        .dataDirectory(datasetDataPath).fileSystem(fileSystem).name(name)
        .schema(schema);

    String partitionExpression = schema.getProp("cdk.partition.expression");

    if (partitionExpression != null) {
      datasetBuilder.partitionExpression(new PartitionExpression(
          partitionExpression));
    }

    return datasetBuilder.get();
  }

  @Override
  public HDFSDataset get(String name) throws IOException {
    Preconditions.checkArgument(name != null, "Dataset name can not be null");
    Preconditions.checkState(rootDirectory != null,
        "Dataset repository root directory can not be null");
    Preconditions.checkState(fileSystem != null,
        "Dataset repository filesystem implementation can not be null");

    logger.debug("Loading dataset:{}", name);

    Path datasetDataPath = pathForDatasetData(name);
    Path datasetMetadataPath = pathForDatasetMetadata(name);

    Schema schema = new Schema.Parser()
        .parse(Paths.toFile(datasetMetadataPath));

    String partitionExpression = schema.getProp("cdk.partition.expression");

    HDFSDataset.Builder datasetBuilder = new HDFSDataset.Builder()
        .fileSystem(fileSystem).dataDirectory(datasetDataPath).name(name)
        .schema(schema);

    if (partitionExpression != null) {
      datasetBuilder.partitionExpression(new PartitionExpression(
          partitionExpression));
    }

    HDFSDataset ds = datasetBuilder.get();

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

  private Path pathForDatasetMetadata(String name) {
    return new Path(pathForDataset(name), "schema.avsc");
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
