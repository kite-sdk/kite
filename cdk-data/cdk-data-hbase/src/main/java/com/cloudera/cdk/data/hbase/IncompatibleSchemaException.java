// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

public class IncompatibleSchemaException extends HBaseCommonException {

  private static final long serialVersionUID = 1L;
  
  public IncompatibleSchemaException(String message) {
    super(message);
  }
  
  public IncompatibleSchemaException(String message, Throwable cause) {
    super(message, cause);
  }
  
  public IncompatibleSchemaException(Throwable cause) {
    super(cause);
  }
}
