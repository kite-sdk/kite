/*
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
package org.kitesdk.morphline.api;

import java.util.ArrayList;
import java.util.List;

import org.kitesdk.morphline.base.Notifications;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public final class Collector implements Command {
  
  private Command parent;
  private List<Record> records;
  private int numStartEvents;
  
  private static final Logger LOG = LoggerFactory.getLogger(Collector.class);
  
  public Collector() {
    reset();
  }
  
  public void reset() {
    records = new ArrayList<Record>();
    numStartEvents = 0;
  }

  @Override
  public Command getParent() {
    return parent;
  }
  
  @Override
  public void notify(Record notification) {
    if (Notifications.containsLifecycleEvent(notification, Notifications.LifecycleEvent.START_SESSION)) {
      numStartEvents++;
    }
  }

  @Override
  public boolean process(Record record) {
    Preconditions.checkNotNull(record);
    records.add(record);
    return true;
  }
  
  public List<Record> getRecords() {
    return records;
  }
  
  public Record getFirstRecord() {
    if (records.size() != 1) {
      throw new IllegalStateException();
    }
    if (records.get(0) == null) {
      throw new IllegalStateException();
    }
    return records.get(0);
  }
  
  public int getNumStartEvents() {
    return numStartEvents;
  }

}
