// (c) Copyright 2011-2012 Cloudera, Inc.
package com.cloudera.cdk.data.hbase.transactions;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

/**
 * Interface for transactions over HBase. Transaction instances should never be
 * created manually. They should only created through a TransactionManager.
 */
public interface Transaction {

  /**
   * Perform a Get over the specified table. The get will only return rows that
   * are valid for the transaction implementation.
   * 
   * @param table
   *          The table to perform the get over
   * @param get
   *          The get to perform
   * @return The result, or null if no result exists.
   */
  public Result get(HTableInterface table, Get get);

  /**
   * Get a result scanner that will fetch rows that are valid for this
   * transaction.
   * 
   * @param table
   *          The table to perform the scan over
   * @param scan
   *          The scan to perform
   * @return A ResultScanner that can scan rows in the table.
   */
  public ResultScanner getScanner(HTableInterface table, Scan scan);

  /**
   * Add a put to the transaction. The put generally won't be executed until
   * commit is called.
   * 
   * @param table
   *          The table to put to
   * @param put
   *          The put to execute
   */
  public void put(HTableInterface table, Put put);

  /**
   * Add a put to the transaction. The put generally won't be executed until
   * commit is called. If occVersion is not null, verify that the current write
   * version in the row is equal to occVersion before performing the put. This
   * is used for optimistic concurrency control.
   * 
   * @param table
   *          The table to put to
   * @param put
   *          The put to execute
   * @param occVersion
   *          The expected version the row must be in for this put to succeed.
   */
  public void put(HTableInterface table, Put put, Long occVersion);

  /**
   * Add a delete to the transaction. The delete generally won't be executed
   * until commit is called.
   * 
   * @param table
   *          The table to delete from
   * @param delete
   *          The delete to execute
   */
  public void delete(HTableInterface table, Delete delete);

  /**
   * Add a delete to the transaction. The delete generally won't be executed
   * until commit is called. If occVersion is not null, verify that the current
   * write version in the row is equal to occVersion before performing the
   * delete. This is used for optimistic concurrency control.
   * 
   * @param table
   *          The table to put to
   * @param delete
   *          The put to execute
   * @param occVersion
   *          The expected version the row must be in for this delete to
   *          succeed.
   */
  public void delete(HTableInterface table, Delete delete, Long occVersion);

  /**
   * Commit the transaction. If commit fails, return false. Otherwise return
   * true.
   * 
   * @return Return true if the commit succeeds.
   */
  public boolean commit();
  
  /**
   * Roll back the transaction. 
   */
  public void rollback();
}
