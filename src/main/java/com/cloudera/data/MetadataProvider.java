package com.cloudera.data;

import java.util.List;

import org.apache.avro.Schema;

public interface MetadataProvider {

  Schema getSchema(String name);

  void setSchema(String name, Schema schema);

  List<String> getPartitionNames(String name);

}
