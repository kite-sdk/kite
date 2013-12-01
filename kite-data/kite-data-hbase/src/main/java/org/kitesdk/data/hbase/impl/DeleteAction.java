/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.hbase.impl;

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
