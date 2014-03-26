/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.morphline.elasticsearch;

import java.util.Collection;
import java.util.Iterator;

public class RoundRobinList<T> {

  private Iterator<T> iterator;
  private final Collection<T> elements;

  public RoundRobinList(Collection<T> elements) {
    this.elements = elements;
    iterator = this.elements.iterator();
  }

  synchronized public T get() {
    if (iterator.hasNext()) {
      return iterator.next();
    } else {
      iterator = elements.iterator();
      return iterator.next();
    }
  }

  public int size() {
    return elements.size();
  }
}
