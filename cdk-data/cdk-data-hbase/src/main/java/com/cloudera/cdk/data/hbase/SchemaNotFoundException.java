// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

public class SchemaNotFoundException extends HBaseCommonException {

  private static final long serialVersionUID = 1L;
  
  public SchemaNotFoundException(String message) {
    super(message);
  }
  
  public SchemaNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
  
  public SchemaNotFoundException(Throwable cause) {
    super(cause);
  }
}