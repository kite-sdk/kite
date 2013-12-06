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
