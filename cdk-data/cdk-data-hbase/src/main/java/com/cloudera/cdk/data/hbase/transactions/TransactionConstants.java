// (c) Copyright 2011-2012 Cloudera, Inc.
package com.cloudera.cdk.data.hbase.transactions;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Various constants for transaction internals
 */
public class TransactionConstants {

  public static final String EPOCH_ORACLE_TABLE_NAME = "epoch";
  
  public static final byte[] EPOCH_ORACLE_TABLE_ROW = new byte[] { 0 };
  
  public static final byte[] EPOCH_ORACLE_COLUMN_FAMLY = Bytes.toBytes("e");
  
  public static final byte[] EPOCH_ORACLE_COLUMN_QUALIFIER = new byte[] {};
  
  public static final byte[] TX_COL_QUALIFIER = new String("tx_e").getBytes();
  
  public static final byte[] TX_LOCK_COL_QUALIFIER = new String("tx_l").getBytes();
  
  public static final int BACKOFF_MS_MULTIPLIER = 10;
  
  public static final int BACKOFF_MAX_CNT = 5;
}
