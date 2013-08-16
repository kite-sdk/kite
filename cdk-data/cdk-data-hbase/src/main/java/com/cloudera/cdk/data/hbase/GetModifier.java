// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

import org.apache.hadoop.hbase.client.Get;

/**
 * Generic callback interface used by HBaseClientTemplate class. This interface
 * modifies a Get instance before the HBaseClientTemplate executes it on the
 * HBase table.
 */
public interface GetModifier {

  /**
   * Modify the Get instance.
   * 
   * @param get
   *          The Get instance to modify.
   * @return The modified Get instance
   */
  public Get modifyGet(Get get);
}
