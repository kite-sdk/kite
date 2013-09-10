// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.dao;

/**
 * An exception that indicates a managed schema modification collided with
 * another client trying to modify that same managed schema.
 */
public class ConcurrentSchemaModificationException extends HBaseCommonException {

  private static final long serialVersionUID = 1L;

  public ConcurrentSchemaModificationException(String msg) {
    super(msg);
  }

  public ConcurrentSchemaModificationException(Throwable cause) {
    super(cause);
  }

  public ConcurrentSchemaModificationException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
