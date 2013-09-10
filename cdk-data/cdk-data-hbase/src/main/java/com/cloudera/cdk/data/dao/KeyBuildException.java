// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.dao;

/**
 * Exception that is thrown when the construction of a Key fails with an error.
 */
public class KeyBuildException extends HBaseCommonException {

  private static final long serialVersionUID = 1L;

  public KeyBuildException(String msg) {
    super(msg);
  }

  public KeyBuildException(Throwable cause) {
    super(cause);
  }

  public KeyBuildException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
