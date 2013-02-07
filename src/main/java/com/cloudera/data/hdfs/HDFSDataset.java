package com.cloudera.data.hdfs;

import java.util.Map;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.data.Dataset;
import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;

public class HDFSDataset implements Dataset {

  private FileSystem fileSystem;
  private Path dataDirectory;

  private String name;
  private Schema schema;
  private Map<String, String> properties;

  public HDFSDataset() {
    properties = Maps.newHashMap();
  }

  public HDFSDatasetWriter getWriter() {
    return new HDFSDatasetWriter(fileSystem, new Path(""), schema);
  }

  public Schema getSchema() {
    return schema;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", name)
        .add("properties", properties).toString();
  }

  public static class Builder implements Supplier<HDFSDataset> {

    private HDFSDataset dataset;

    public Builder() {
      dataset = new HDFSDataset();
    }

    public Builder fileSystem(FileSystem fileSystem) {
      dataset.fileSystem = fileSystem;
      return this;
    }

    public Builder name(String name) {
      dataset.name = name;
      return this;
    }

    public Builder dataDirectory(Path dataDirectory) {
      dataset.dataDirectory = dataDirectory;
      return this;
    }

    public Builder schema(Schema schema) {
      dataset.schema = schema;
      return this;
    }

    public Builder properties(Map<String, String> properties) {
      dataset.properties = properties;
      return this;
    }

    @Override
    public HDFSDataset get() {
      HDFSDataset current = dataset;
      dataset = new HDFSDataset();

      return current;
    }

  }

}
