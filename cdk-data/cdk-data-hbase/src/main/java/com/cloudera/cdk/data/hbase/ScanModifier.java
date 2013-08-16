// (c) Copyright 2011-2012 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

import org.apache.hadoop.hbase.client.Scan;

/**
 * Generic callback interface used by HBaseClientTemplate class. This interface
 * modifies a Scan instance before the HBaseClientTemplate executes it on the
 * HBase table.
 */
public interface ScanModifier {

  /**
   * Modify the Scan instance.
   * 
   * @param scan
   *          The Scan instance to modify
   * @return The modified Scan instance
   */
  public Scan modifyScan(Scan scan);
}
