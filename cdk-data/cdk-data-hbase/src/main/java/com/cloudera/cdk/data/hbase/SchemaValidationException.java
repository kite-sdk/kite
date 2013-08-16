// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

public class SchemaValidationException extends HBaseCommonException {
  
  private static final long serialVersionUID = 1L;

  public SchemaValidationException(String msg) {
    super(msg);
  }

  public SchemaValidationException(Throwable cause) {
    super(cause);
  }

  public SchemaValidationException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
