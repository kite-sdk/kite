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

package org.kitesdk.data.spi;

import com.google.common.base.Objects;

public class Pair<K, V> {

  private final K first;
  private final V second;

  public static <T, U> Pair<T, U> of(T first, U second) {
    return new Pair<T, U>(first, second);
  }

  public Pair(K first, V second) {
    this.first = first;
    this.second = second;
  }

  public K first() {
    return first;
  }

  public V second() {
    return second;
  }

  public Object get(int index) {
    switch (index) {
      case 0:
        return first;
      case 1:
        return second;
      default:
        throw new ArrayIndexOutOfBoundsException();
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(first, second);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Pair<?, ?> other = (Pair<?, ?>) obj;
    return (Objects.equal(first, other.first)
        && Objects.equal(second, other.second));
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("first", first)
        .add("second", second)
        .toString();
  }
}
