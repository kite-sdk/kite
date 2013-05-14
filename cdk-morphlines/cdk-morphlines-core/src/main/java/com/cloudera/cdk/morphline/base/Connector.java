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
package com.cloudera.cdk.morphline.base;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.Record;
import com.google.common.base.Preconditions;

/**
 * Command that is sandwiched between two other commands, chaining the two other commands together.
 */
final class Connector implements Command {
  
  private Command parent;
  private Command child;
  private boolean ignoreNotifications;
  
  public Connector() {
    this(false);
  }

  public Connector(boolean ignoreNotifications) {
    this.ignoreNotifications = ignoreNotifications;
  }

  @Override
  public Command getParent() {
    return parent;
  }
  
  public void setParent(Command parent) {
    this.parent = parent;
  }
  
  public void setChild(Command child) {
    this.child = child;
  }

  @Override
  public void notify(Record notification) {
    Preconditions.checkNotNull(parent);
    Preconditions.checkNotNull(child);
    if (!ignoreNotifications) {
      child.notify(notification);
    }
  }

  @Override
  public boolean process(Record record) {
    Preconditions.checkNotNull(record);
    return child.process(record);
  }

}
