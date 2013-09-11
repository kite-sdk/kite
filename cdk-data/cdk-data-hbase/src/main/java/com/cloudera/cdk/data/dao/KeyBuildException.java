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
package com.cloudera.cdk.data.dao;

/**
 * Exception that is thrown when the construction of a Key fails with an error.
 */
public class KeyBuildException extends HBaseCommonException {

  private static final long serialVersionUID = 1L;

  public KeyBuildException(String msg) {
    super(msg);
  }

  public KeyBuildException(Throwable cause) {
    super(cause);
  }

  public KeyBuildException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
