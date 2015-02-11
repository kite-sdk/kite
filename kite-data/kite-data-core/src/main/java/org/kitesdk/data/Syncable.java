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
 * A writer that can guarantee data is persisted to disk.
 * <p>
 * When {@link #sync} returns, data already written is flushed to data nodes
 * responsible for replicas and persisted to disk, not just to the underlying
 * OS buffer.
 *
 * @since 0.18.0
 */
public interface Syncable {
  /**
   * Ensure that data has been synced to disk on all replica data nodes.
   */
  void sync();
}
