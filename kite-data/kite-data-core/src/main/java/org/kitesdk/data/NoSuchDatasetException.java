/*
 * Copyright 2013 Cloudera.
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
package org.kitesdk.data;

/**
 * Exception thrown when a DatasetRepository method is incorrectly called for a
 * dataset name that does not yet exist.
 * @since 0.7.0
 * @deprecated will be removed in 0.11.0, use {@link DatasetNotFoundException} instead
 */
public class NoSuchDatasetException extends DatasetNotFoundException {

  public NoSuchDatasetException(String message) {
    super(message);
  }

  public NoSuchDatasetException(String message, Throwable cause) {
    super(message, cause);
  }

  public NoSuchDatasetException(Throwable cause) {
    super(cause);
  }
}
