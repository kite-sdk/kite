/*
 * Copyright 2015 Cloudera Inc.
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

package org.kitesdk.data.spi;

/**
 * This interface is for classes that can replace parts of themselves in some
 * (undefined) way. Once replaced, the update can be discarded without losing
 * information.
 *
 * @param <T> the type of the object to replace
 */
public interface Replaceable<T> {
  /**
   * Check whether {@code part} can be replaced.
   *
   * @param part the object to replace parts of this
   * @return {@code true} if the object can replace parts of this
   */
  boolean canReplace(T part);

  /**
   * Replace part of {@code this} with the {@code replacement} object.
   *
   * @param replacement the object to replace parts of this
   */
  void replace(T target, T replacement);
}
