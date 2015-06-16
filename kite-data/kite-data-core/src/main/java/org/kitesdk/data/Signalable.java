/*
 * Copyright 2015 Cloudera.
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
 * Signalable views may signal consumers when their underlying data is ready for
 * consumption. Not all View implementations provide this capability.
 *
 * @since 1.1.0
 */
public interface Signalable<E> extends View<E> {

  /**
   * Signal that the view's data is ready for consumption.
   *
   * Note that an {@link #isEmpty() empty} view may be signaled as ready.
   *
   * @since 1.1.0
   */
  public void signalReady();

  /**
   * Returns {@code true} if the view's data is ready for consumption.
   *
   * A view is considered ready if
   * <ul>
   * <li>it has been {@link #signalReady() signaled ready}</li>
   * <li>it is a subset of a ready view (may not be implemented)</li>
   * <li>
   *   it is completely covered by a union of ready views, or is a subset of such
   *   a union (may not be implemented)
   * </li>
   * <ul>
   *
   * Note that ready views may also be {@link #isEmpty() empty}.
   *
   * @since 1.1.0
   */
  public boolean isReady();

}
