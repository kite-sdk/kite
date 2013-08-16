// (c) Copyright 2011-2012 Cloudera, Inc.
package com.cloudera.cdk.data.hbase.transactions;

import org.apache.hadoop.hbase.client.HTableInterface;

/**
 * The TransactionManager is responsible for serving Transactions to clients.
 * This TransactionManager knows how to construct the different supported
 * Transaction types we support.
 * 
 * The TransactionManager is also responsible for serving certain Transaction
 * management tasks to the appropriate transaction type. For example, cleaning
 * up a certain transaction type that might be stuck in a locked state.
 * 
 * TransactionManager implementations should extend this implementation.
 */
public interface TransactionManager {

  public abstract Transaction getTransaction();

  public abstract Transaction getTransaction(TransactionType transactionType);

  public abstract boolean commit(Transaction transaction);

  public abstract void rollback(Transaction transaction);

  public EpochOracleClient getEpochOracleClient();

  /**
   * Clean a transaction that is in a locked state. This is the responsibility
   * of the Transaction of the type that the Transaction was written with, and
   * it's this method which calls the appropriate Transaction's cleanup method.
   * 
   * @param table
   *          The table the locked row is in.
   * @param row
   *          The locked row
   * @param transactionEntity
   *          The TransactionEntity of the locked row.
   */
  public void cleanTransaction(HTableInterface table, byte[] row,
      TransactionEntity transactionEntity);
}
