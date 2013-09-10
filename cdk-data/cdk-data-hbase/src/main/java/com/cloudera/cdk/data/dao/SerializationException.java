// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.dao;

public class SerializationException extends HBaseCommonException {

  private static final long serialVersionUID = 1L;
  
  public SerializationException(String message, Throwable root) {
    super(message, root);
  }
}
