// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

/**
 * The base Exception type for HBase Common.
 */
public class HBaseCommonException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public HBaseCommonException(Throwable root) {
    super(root);
  }

  public HBaseCommonException(String string, Throwable root) {
    super(string, root);
  }

  public HBaseCommonException(String s) {
    super(s);
  }
}
