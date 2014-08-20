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

package org.kitesdk.tools;

import org.apache.crunch.fn.IdentityFn;
import org.kitesdk.data.View;

public class CopyTask<E> extends TransformTask<E, E> {

  /**
   * @since 0.16.0
   */
  public CopyTask(View<E> from, View<E> to) {
    super(from, to, IdentityFn.<E>getInstance());
  }

  /**
   * @deprecated will be removed in 0.17.0; use CopyTask(View, View)
   */
  @Deprecated
  public CopyTask(View<E> from, View<E> to, Class<E> entityClass) {
    super(from, to, IdentityFn.<E>getInstance());
  }

}
