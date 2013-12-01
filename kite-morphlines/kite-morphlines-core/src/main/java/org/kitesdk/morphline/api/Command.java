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

/**
 * A command transforms a record into zero or more records.
 * 
 * A command has a boolean return code, indicating success or failure. All record handlers in a
 * morphline implement this interface. Commands are chained together. The parent of a command A is
 * the command B that passes records to A. A is the child of B.
 * 
 * Data is sent on the data plane whereas notifications are sent on the control plane, which is a
 * separate communication channel.
 */
public interface Command {
  
  /**
   * Processes the given notification on the control plane of the subtree rooted at this command.
   */
  void notify(Record notification);
  
  /**
   * Processes the given record on the data plane of this command.
   * 
   * @return true to indicate that processing shall continue, false to indicate that backtracking
   *         shall be done
   */
  boolean process(Record record);
  
  /**
   * Returns the parent of this command. The parent of a command A is the command B that passes
   * records to A. A is the child of B.
   */
  Command getParent();
}
