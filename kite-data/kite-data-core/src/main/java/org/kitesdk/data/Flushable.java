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
 * A writer that can guarantee data is present on data nodes.
 * <p>
 * {@link #flush} has a stronger guarantee than {@link java.io.Flushable#flush}.
 * When it returns, data already written is flushed to the OS buffers on all
 * data nodes responsible for replicas.
 * <p>
 * Once data has been flushed, it will be tolerant to single-node and rack
 * failures.
 *
 * @since 0.18.0
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value="NM_SAME_SIMPLE_NAME_AS_INTERFACE",
    justification="Intended to be a stricter version of the parent interface")
public interface Flushable extends java.io.Flushable {
  /**
   * Ensure that data has been flushed to OS buffers on all replica data nodes.
   * <p>
   * This method has a stronger guarantee than {@link java.io.Flushable#flush}.
   * When this method returns, data already written has been flushed to the OS
   * buffers on all data nodes responsible for replicas.
   */
  @Override
  void flush();
}
