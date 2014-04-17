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
package org.kitesdk.data;

/**
 * <p>
 * Exception thrown to indicate that there is a problem
 * finding a particular version of a schema.
 * <p>
 * @since 0.9.0
 */
public class SchemaNotFoundException extends DatasetException {

  public SchemaNotFoundException(String message) {
    super(message);
  }
  
  public SchemaNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
  
  public SchemaNotFoundException(Throwable cause) {
    super(cause);
  }
}
