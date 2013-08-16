// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

import org.apache.hadoop.hbase.client.Put;

/**
 * Represents an HBase Put. This simple class encapsulates both a Put, and a
 * VersionCheckAction operation, which allows us to do checkAndPut.
 */
public class PutAction {

  private final Put put;
  private VersionCheckAction versionCheckAction;

  public PutAction(Put put) {
    this(put, null);
  }

  public PutAction(Put put, VersionCheckAction versionCheckAction) {
    this.put = put;
    this.versionCheckAction = versionCheckAction;
  }

  public Put getPut() {
    return put;
  }

  public VersionCheckAction getVersionCheckAction() {
    return versionCheckAction;
  }

  public void setVersionCheckAction(VersionCheckAction versionCheckAction) {
    this.versionCheckAction = versionCheckAction;
  }
}
