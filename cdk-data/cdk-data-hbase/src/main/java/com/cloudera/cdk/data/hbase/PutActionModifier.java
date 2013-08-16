// (c) Copyright 2011-2012 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

/**
 * Generic callback interface used by HBaseClientTemplate class. This interface
 * modifies a Put instance before the HBaseClientTemplate executes it on the
 * HBase table.
 */
public interface PutActionModifier {

  /**
   * Modify the PutAction instance.
   * 
   * @param putAction
   *          The PutAction instance to modify.
   * @return The modified PutAction
   */
  public PutAction modifyPutAction(PutAction putAction);
}
