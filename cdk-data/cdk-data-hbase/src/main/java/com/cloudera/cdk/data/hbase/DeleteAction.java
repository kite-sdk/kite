// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

import org.apache.hadoop.hbase.client.Delete;

/**
 * Represents a Delete. This simple class encapsulates both a Delete, and a
 * CheckAction operation, which allows us to do checkAndDelete.
 */
public class DeleteAction {

  private final Delete delete;
  private VersionCheckAction versionCheckAction;

  public DeleteAction(Delete delete) {
    this(delete, null);
  }

  public DeleteAction(Delete delete, VersionCheckAction versionCheckAction) {
    this.delete = delete;
    this.versionCheckAction = versionCheckAction;
  }

  public Delete getDelete() {
    return delete;
  }

  public VersionCheckAction getVersionCheckAction() {
    return versionCheckAction;
  }

  public void setVersionCheckAction(VersionCheckAction versionCheckAction) {
    this.versionCheckAction = versionCheckAction;
  }
}
