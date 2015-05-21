/*
 * Copyright 2015 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.spark

/*
 /**
  * Check to see if the output target already exists before running
  * the pipeline, and if it does, print an error and throw an exception.
  */
 DEFAULT,

 /**
  * Check to see if the output target already exists, and if it does,
  * delete it and overwrite it with the new output (if any).
  */
 OVERWRITE,

 /**
  * If the output target does not exist, create it. If it does exist,
  * add the output of this pipeline to the target. This was the
  * behavior in Crunch up to version 0.4.0.
  */
 APPEND,
*/

object WriteMode extends Enumeration {

  type WriteMode = Value

  val

  /**
   * Check to see if the output target already exists before running
   * the pipeline, and if it does, print an error and throw an exception.
   */
  DEFAULT,

  /**
   * Check to see if the output target already exists, and if it does,
   * delete it and overwrite it with the new output (if any).
   */
  OVERWRITE,

  /**
   * If the output target does not exist, create it. If it does exist,
   * add the output of this pipeline to the target. This was the
   * behavior in Crunch up to version 0.4.0.
   */
  APPEND = Value
}
