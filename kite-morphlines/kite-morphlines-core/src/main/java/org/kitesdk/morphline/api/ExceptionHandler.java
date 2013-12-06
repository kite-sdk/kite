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
 * Morphline-wide default handler that commands can choose to use to handle exceptions.
 * 
 * Mission critical, large-scale online production systems need to make progress without downtime
 * despite some issues. Thus, the recommendation is that implementations of this handler rethrow
 * exceptions in test mode, but try to log and continue in production mode, if that's considered
 * appropriate and feasible.
 */ 
public interface ExceptionHandler {
    
  /** Handle the given exception resulting from the given input record (the record can be null) */
  public void handleException(Throwable t, Record record);
    
}
