package com.cloudera.data.hdfs;

import java.io.Closeable;
import java.io.DataInputStream;
import java.util.Map;

import org.apache.avro.Schema;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;

public class Dataset implements Closeable {

  private String name;
  private String path;
  private Schema schema;
  private Map<String, String> properties;

  public Dataset() {
    properties = Maps.newHashMap();
  }

  public DataInputStream open() {
    return null;
  }

  @Override
  public void close() {

  }

  public Schema getSchema() {
    return schema;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", name).add("path", path)
        .add("properties", properties).toString();
  }

}
