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


import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.kitesdk.data.spi.CharSequences;
import org.kitesdk.data.spi.SchemaUtil;

public class Range<T> implements Predicate<T> {

  public static final Pattern RANGE_PATTERN = Pattern.compile(
      "([\\[\\(])([^,]*),([^\\]\\)]*)([\\]\\)])");

  public static <T> Range<T> fromString(String range, Schema schema) {
    Matcher match = RANGE_PATTERN.matcher(range);
    if (match.matches()) {
      boolean lIncl = "[".equals(match.group(1));
      T lower;
      if (match.group(2).isEmpty()) {
        lower = null;
      } else {
        lower = SchemaUtil.fromString(match.group(2), schema);
      }
      T upper;
      if (match.group(3).isEmpty()) {
        upper = null;
      } else {
        upper = SchemaUtil.fromString(match.group(3), schema);
      }
      boolean uIncl = "]".equals(match.group(4));
      return new Range<T>(bound(lower, lIncl), bound(upper, uIncl));
    } else {
      // not a Range
      return null;
    }
  }

  private final Bound<T> lower;
  private final Bound<T> upper;

  @SuppressWarnings("unchecked")
  Range(Bound<T> lower, Bound<T> upper) {
    this.lower = (lower == null ? ((Bound<T>) INF) : lower);
    this.upper = (upper == null ? ((Bound<T>) INF) : upper);
  }

  public boolean isLowerBoundOpen() {
    return !lower.inclusive();
  }

  public boolean isUpperBoundOpen() {
    return !upper.inclusive();
  }

  public boolean isLowerBoundClosed() {
    return lower.inclusive();
  }

  public boolean isUpperBoundClosed() {
    return upper.inclusive();
  }

  public boolean hasLowerBound() {
    return lower != INF;
  }

  public boolean hasUpperBound() {
    return upper != INF;
  }

  public T lowerEndpoint() {
    return lower.endpoint();
  }

  public T upperEndpoint() {
    return upper.endpoint();
  }

  public Range<T> intersection(Range<T> other) {
    Bound<T> newLower;
    if (other.lower == INF) {
      newLower = lower;
    } else {
      newLower = lower.isLessThan(other.lower.endpoint()) ? other.lower : lower;
    }
    Bound<T> newUpper;
    if (other.upper == INF) {
      newUpper = upper;
    } else {
      newUpper = upper.isGreaterThan(other.upper.endpoint()) ? other.upper : upper;
    }
    return new Range<T>(newLower, newUpper);
  }

  @Override
  public boolean apply(@Nullable T input) {
    return contains(input);
  }

  public boolean contains(T input) {
    return lower.isLessThan(input) && upper.isGreaterThan(input);
  }

  @Override
  public String toString() {
    return (isLowerBoundClosed() ? "[" : "(") +
        lower.toString() + ", " + upper.toString() +
        (isUpperBoundClosed() ? "]" : ")");
  }

  public String toString(Schema schema) {
    return (isLowerBoundClosed() ? "[" : "(") +
        lower.toString(schema) + "," + upper.toString(schema) +
        (isUpperBoundClosed() ? "]" : ")");
  }

  @Override
  public boolean equals(@Nullable Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || object.getClass() != getClass()) {
      return false;
    }
    Range other = (Range) object;
    return Objects.equal(lower, other.lower) &&
        Objects.equal(upper, other.upper);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(lower, upper);
  }

  @SuppressWarnings("unchecked")
  static <T> Bound<T> bound(T endpoint, boolean inclusive) {
    if (endpoint == null) {
      return (Bound<T>) INF;
    } else if (endpoint instanceof CharSequence) {
      return (Bound<T>) new CharSequenceBound((CharSequence) endpoint, inclusive);
    } else if (endpoint instanceof Comparable) {
      return (Bound<T>) new ComparableBound((Comparable) endpoint, inclusive);
    } else {
      throw new RuntimeException();
    }
  }

  private static interface Bound<T> {
    public boolean inclusive();
    public T endpoint();
    public boolean isLessThan(T other);
    public boolean isGreaterThan(T other);
    public String toString(Schema schema);
  }

  private static Bound<Object> INF = new Bound<Object>() {
    @Override
    public boolean inclusive() {
      return false;
    }

    @Override
    public Object endpoint() {
      return null;
    }

    @Override
    public boolean isLessThan(Object other) {
      return true;
    }

    @Override
    public boolean isGreaterThan(Object other) {
      return true;
    }

    @Override
    public String toString() {
      return "inf";
    }

    @Override
    public String toString(Schema _) {
      return "";
    }
  };

  private abstract static class AbstractBound<T> implements Bound<T> {
    private final T endpoint;
    private final boolean inclusive;

    private AbstractBound(T endpoint, boolean inclusive) {
      this.endpoint = endpoint;
      this.inclusive = inclusive;
    }

    protected abstract int compare(T left, T right);

    public boolean inclusive() {
      return inclusive;
    }

    public T endpoint() {
      return endpoint;
    }

    public boolean isLessThan(T other) {
      int cmp = compare(endpoint, other);
      if (cmp < 0) {
        return true;
      } else if (cmp == 0) {
        return inclusive;
      }
      return false;
    }

    public boolean isGreaterThan(T other) {
      int cmp = compare(other, endpoint);
      if (cmp < 0) {
        return true;
      } else if (cmp == 0) {
        return inclusive;
      }
      return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || obj.getClass() != getClass()) {
        return false;
      }
      AbstractBound<T> other = (AbstractBound<T>) obj;
      return (inclusive == other.inclusive) &&
          (compare(endpoint, other.endpoint) == 0);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(inclusive, endpoint);
    }

    @Override
    public String toString() {
      return endpoint.toString();
    }

    @Override
    public String toString(Schema schema) {
      return SchemaUtil.toString(endpoint, schema);
    }
  }

  private static class CharSequenceBound extends AbstractBound<CharSequence> {
    private CharSequenceBound(CharSequence endpoint, boolean inclusive) {
      super(endpoint, inclusive);
    }

    @Override
    protected int compare(CharSequence left, CharSequence right) {
      return CharSequences.compare(left, right);
    }

    @Override
    public boolean equals(Object obj) {
      // without this, findbugs complains. correct because super calls compare.
      return super.equals(obj);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(inclusive(), CharSequences.hashCode(endpoint()));
    }
  }

  private static class ComparableBound<C extends Comparable<C>> extends AbstractBound<C> {
    private ComparableBound(C endpoint, boolean inclusive) {
      super(endpoint, inclusive);
    }

    @Override
    protected int compare(C left, C right) {
      return left.compareTo(right);
    }
  }
}
