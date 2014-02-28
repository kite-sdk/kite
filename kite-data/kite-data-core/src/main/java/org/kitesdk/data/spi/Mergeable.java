/*
 * Copyright 2014 Cloudera.
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
 * This interface is for classes that can merge updates into themselves in some
 * (undefined) way. Once merged, the update can be discarded without losing information.
 *
 * @param <T> the type of the object to merge
 */
public interface Mergeable<T> {
  /**
   * Merge the <code>update</code> object into <code>this</code>.
   *
   * @param update the object to merge into this one
   */
  public void merge(T update);
}
