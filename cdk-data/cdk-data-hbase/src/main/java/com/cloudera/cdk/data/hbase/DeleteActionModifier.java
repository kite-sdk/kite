// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

/**
 * Generic callback interface used by HBaseClientTemplate class. This interface
 * modifies a Delete instance before the HBaseClientTemplate executes it on the
 * HBase table.
 */
public interface DeleteActionModifier {

  /**
   * Modify the DeleteAction instance.
   * 
   * @param deleteAction
   *          The DeleteAction instance to modify.
   * @return The modified DeleteAction
   */
  public DeleteAction modifyDeleteAction(DeleteAction deleteAction);
}
