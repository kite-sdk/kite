// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase.filters;

import org.apache.hadoop.hbase.filter.Filter;

/**
 * An interface for creating server side HBase filters. 
 */
public interface EntityFilter {

  /**
   * Returns The HBase Filter Object
   * @return Filter
   */
  public Filter getFilter();
}
