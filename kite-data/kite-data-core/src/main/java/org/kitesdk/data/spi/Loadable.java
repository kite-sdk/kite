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
package org.kitesdk.data.spi;

/**
 * This interface is used to load classes via a {@link java.util.ServiceLoader}.
 *
 * Implementations of this interface may perform loading or registration tasks
 * within the {@link #load()} method and must also define a no-argument
 * constructor.
 *
 * For implementations to be automatically loaded, they must be listed in:
 *   META-INF/services/org.kitesdk.data.spi.Loadable
 *
 * @since 0.8.0
 */
public interface Loadable {
  /**
   * Performs any loading and registration tasks.
   */
  public void load();
}
