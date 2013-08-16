// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

/**
 * A version check action, which allows us to check that the
 * version of an entity hasn't changed. This is useful for
 * optimistic concurrency control (OCC).
 */
public class VersionCheckAction {

  private final long version;
  
  public VersionCheckAction(long version) {
    this.version = version;
  }
  
  public long getVersion() {
    return version;
  }
}
