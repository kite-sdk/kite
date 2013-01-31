package com.cloudera.data.hdfs;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;

public class HDFSDatasetRepository implements DatasetRepository {

  private static final Logger logger = LoggerFactory
      .getLogger(HDFSDatasetRepository.class);

  private Path rootDirectory;
  private FileSystem fileSystem;

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

  public Dataset create(String name, Schema schema) throws IOException {
    Preconditions.checkArgument(name != null, "Dataset name can not be null");
    Preconditions.checkArgument(schema != null,
        "Dataset schema can not be null");
    Preconditions.checkState(fileSystem != null,
        "Dataset repository filesystem implementation can not be null");

    Dataset ds = new Dataset();
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

    Files.write(schema.toString(), new File(datasetMetadataPath.toUri()
        .getPath()), Charsets.UTF_8);

    ds.setSchema(schema);

    return ds;
  }

  @Override
  public Dataset get(String name) {
    Preconditions.checkArgument(name != null, "Dataset name can not be null");
    Preconditions.checkState(rootDirectory != null,
        "Dataset repository root directory can not be null");
    Preconditions.checkState(fileSystem != null,
        "Dataset repository filesystem implementation can not be null");

    logger.debug("Loading dataset:{}", name);

    Path datasetMetadataPath = pathForDatasetMetadata(name);

    Dataset ds = new Dataset();

    Schema schema = new Schema.Parser().parse(datasetMetadataPath.toUri()
        .getPath());

    ds.setSchema(schema);

    logger.debug("Loaded dataset:{}", ds);

    return ds;
  }

  @Override
  public Dataset get(String name, long version) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("rootDirectory", rootDirectory)
        .add("fileSystem", fileSystem).toString();
  }

  public Path getRootDirectory() {
    return rootDirectory;
  }

  public void setRootDirectory(Path rootDirectory) {
    this.rootDirectory = rootDirectory;
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }

  public void setFileSystem(FileSystem fileSystem) {
    this.fileSystem = fileSystem;
  }

}
