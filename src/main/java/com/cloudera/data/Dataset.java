package com.cloudera.data;

import org.apache.avro.Schema;

public interface Dataset {

  String getName();

  Schema getSchema();

}
