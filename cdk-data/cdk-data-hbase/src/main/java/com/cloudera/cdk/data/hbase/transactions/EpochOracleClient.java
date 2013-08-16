// (c) Copyright 2011-2012 Cloudera, Inc.
package com.cloudera.cdk.data.hbase.transactions;


/**
 * An EpochOracleClient is a client that can interact with an
 * EpochOracle. EpochOracle's return an epoch, which is a strictly
 * increasing timestamp.
 */
public interface EpochOracleClient {

  /**
   * Get the next strictly increasing timestamp.
   * 
   * @return The epoch
   */
  public long getEpoch();
}
