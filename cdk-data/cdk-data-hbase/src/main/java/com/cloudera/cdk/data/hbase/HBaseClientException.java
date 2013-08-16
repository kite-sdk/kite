// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

import java.io.IOException;

/**
 * Wraps IOExceptions exceptions thrown by the HBase Client API.
 */
public class HBaseClientException extends HBaseCommonException {

  private static final long serialVersionUID = 1L;
  private final IOException ioException;
  
  public HBaseClientException(String message, IOException root) {
    super(message, root);
    this.ioException = root;
  }
  
  public IOException getIOException() {
    return ioException;
  }
}
