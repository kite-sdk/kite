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

import java.util.Map;

/**
 * This interface defines methods that allow instances of some type, T, to be
 * created from a set of options. Options are passed as a Map from String names
 * to String values.
 *
 * @param <T> The type of objects returned by the
 *            {@link #getFromOptions(java.util.Map)} method.
 *
 * @since 0.8.0
 */
public interface OptionBuilder<T> {
  /**
   * Build and return an instance of T configured from the given set of Options.
   *
   * @param options a Map from String names to String values.
   * @return a configured instance of T
   */
  public T getFromOptions(Map<String, String> options);
}
