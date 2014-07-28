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

package org.kitesdk.data.spi.predicates;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.BoundType;
import com.google.common.collect.DiscreteDomain;
import java.util.Set;

public class Ranges {
  public static <T> Range<T> open(T lower, T upper) {
    return new Range<T>(Range.bound(lower, false), Range.bound(upper, false));
  }

  public static <T> Range<T> closed(T lower, T upper) {
    return new Range<T>(Range.bound(lower, true), Range.bound(upper, true));
  }

  public static <T> Range<T> closedOpen(T lower, T upper) {
    return new Range<T>(Range.bound(lower, true), Range.bound(upper, false));
  }

  public static <T> Range<T> openClosed(T lower, T upper) {
    return new Range<T>(Range.bound(lower, false), Range.bound(upper, true));
  }

  public static <T> Range<T> greaterThan(T endpoint) {
    return new Range<T>(Range.bound(endpoint, false), null);
  }

  public static <T> Range<T> atLeast(T endpoint) {
    return new Range<T>(Range.bound(endpoint, true), null);
  }

  public static <T> Range<T> lessThan(T endpoint) {
    return new Range<T>(null, Range.bound(endpoint, false));
  }

  public static <T> Range<T> atMost(T endpoint) {
    return new Range<T>(null, Range.bound(endpoint, true));
  }

  public static <T> Range<T> all() {
    return new Range<T>(null, null);
  }

  public static <T> Range<T> singleton(T endpoint) {
    return new Range<T>(Range.bound(endpoint, true), Range.bound(endpoint, true));
  }

  public static <S extends Comparable, T extends Comparable>
  Range<T> transformClosed(Range<S> range, Function<? super S, T> function) {
    if (range.hasLowerBound()) {
      if (range.hasUpperBound()) {
        return closed(
            function.apply(range.lowerEndpoint()),
            function.apply(range.upperEndpoint()));
      } else {
        return atLeast(function.apply(range.lowerEndpoint()));
      }
    } else if (range.hasUpperBound()) {
      return atMost(function.apply(range.upperEndpoint()));
    } else {
      return null;
    }
  }

  public static <T extends Comparable>
  Range<T> adjustClosed(Range<T> range, DiscreteDomain<T> domain) {
    // adjust to a closed range to avoid catching extra keys
    if (range.hasLowerBound()) {
      T lower = range.lowerEndpoint();
      if (range.isLowerBoundOpen()) {
        lower = domain.next(lower);
      }
      if (range.hasUpperBound()) {
        T upper = range.upperEndpoint();
        if (range.isUpperBoundOpen()) {
          upper = domain.previous(upper);
        }
        return closed(lower, upper);
      } else {
        return atLeast(lower);
      }
    } else if (range.hasUpperBound()) {
      T upper = range.upperEndpoint();
      if (range.isUpperBoundOpen()) {
        upper = domain.previous(upper);
      }
      return atMost(upper);
    } else {
      throw new IllegalArgumentException("Invalid range: no endpoints");
    }
  }

  public static <C extends Comparable<C>> Set<C> asSet(
      Range<C> range, DiscreteDomain<C> domain) {
    // cheat and pass this to guava
    return asGuavaRange(range).asSet(domain);
  }

  @VisibleForTesting
  static <C extends Comparable<C>> com.google.common.collect.Range<C> asGuavaRange(
      Range<C> range) {
    if (range.hasLowerBound()) {
      if (range.hasUpperBound()) {
        return com.google.common.collect.Ranges.range(
            range.lowerEndpoint(),
            range.isLowerBoundOpen() ? BoundType.OPEN : BoundType.CLOSED,
            range.upperEndpoint(),
            range.isUpperBoundOpen() ? BoundType.OPEN : BoundType.CLOSED);
      } else {
        return com.google.common.collect.Ranges.downTo(
            range.lowerEndpoint(),
            range.isLowerBoundOpen() ? BoundType.OPEN : BoundType.CLOSED);
      }
    } else if (range.hasUpperBound()) {
      return com.google.common.collect.Ranges.upTo(
          range.upperEndpoint(),
          range.isUpperBoundOpen() ? BoundType.OPEN : BoundType.CLOSED);
    } else {
      return com.google.common.collect.Ranges.all();
    }
  }
}
