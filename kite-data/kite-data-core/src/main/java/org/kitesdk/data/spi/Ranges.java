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

public class Ranges {
  public static <T> Range<T> open(T lower, T upper) {
    return Range.open(lower, upper);
  }

  public static <T> Range<T> closed(T lower, T upper) {
    return Range.closed(lower, upper);
  }

  public static <T> Range<T> closedOpen(T lower, T upper) {
    return Range.closedOpen(lower, upper);
  }

  public static <T> Range<T> openClosed(T lower, T upper) {
    return Range.openClosed(lower, upper);
  }

  public static <T> Range<T> greaterThan(T endpoint) {
    return Range.greaterThan(endpoint);
  }

  public static <T> Range<T> atLeast(T endpoint) {
    return Range.atLeast(endpoint);
  }

  public static <T> Range<T> lessThan(T endpoint) {
    return Range.lessThan(endpoint);
  }

  public static <T> Range<T> atMost(T endpoint) {
    return Range.atMost(endpoint);
  }

  public static <T> Range<T> singleton(T endpoint) {
    return Range.singleton(endpoint);
  }
}
