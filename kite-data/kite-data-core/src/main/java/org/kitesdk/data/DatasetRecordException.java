/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data;

/**
 * A DatasetException that signals that an individual record has failed.
 * <p>
 * When writing, this exception indicates that a record could not be written,
 * but the writer is still valid and ready to write other records.
 * <p>
 * When reading, this exception indicates that a record could not be built from
 * record data, but that reader is still valid and able to read other records.
 *
 * @since 0.18.0
 */
public class DatasetRecordException extends DatasetException {
  public DatasetRecordException(String message) {
    super(message);
  }

  public DatasetRecordException(String message, Throwable t) {
    super(message, t);
  }

  /**
   * Precondition-style validation that throws a {@link DatasetRecordException}.
   *
   * @param isValid
   *          {@code true} if valid, {@code false} if an exception should be
   *          thrown
   * @param message
   *          A String message for the exception.
   */
  public static void check(boolean isValid, String message, Object... args) {
    if (!isValid) {
      String[] argStrings = new String[args.length];
      for (int i = 0; i < args.length; i += 1) {
        argStrings[i] = String.valueOf(args[i]);
      }
      throw new DatasetRecordException(
          String.format(String.valueOf(message), (Object[]) argStrings));
    }
  }
}
