/**
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
package org.kitesdk.data.spi.partition;

import com.google.common.base.Predicate;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.DiscreteDomains;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.kitesdk.data.spi.FieldPartitioner;
import com.google.common.base.Objects;
import org.kitesdk.data.spi.predicates.Exists;
import org.kitesdk.data.spi.predicates.In;
import org.kitesdk.data.spi.predicates.Predicates;
import org.kitesdk.data.spi.predicates.Range;
import org.kitesdk.data.spi.predicates.Ranges;

@edu.umd.cs.findbugs.annotations.SuppressWarnings(value={
        "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE",
        "SE_COMPARATOR_SHOULD_BE_SERIALIZABLE"},
    justification="False positive due to generics.")
@Immutable
public class RangeFieldPartitioner extends FieldPartitioner<String, String> {

  private final List<String> upperBounds;

  // lazily constructed DiscreteDomain for upper bounds; use domain()
  private final RangeDomain domain;

  public RangeFieldPartitioner(String sourceName, String... upperBounds) {
    this(sourceName, null, upperBounds);
  }

  public RangeFieldPartitioner(String sourceName, @Nullable String name,
                               String... upperBounds) {
    super(sourceName, (name == null ? sourceName + "_bound" : name),
        String.class, String.class, upperBounds.length);
    this.upperBounds = ImmutableList.copyOf(upperBounds);
    this.domain = new RangeDomain();
  }

  @Override
  public String apply(String value) {
    // always return the same String object so identity comparison can be used
    for (String upper : upperBounds) {
      if (value.compareTo(upper) <= 0) {
        return upper;
      }
    }
    throw new IllegalArgumentException(value + " is outside bounds");
  }

  @Override
  public Predicate<String> project(Predicate<String> predicate) {
    if (predicate instanceof Exists) {
      return Predicates.exists();
    } else if (predicate instanceof In) {
      return ((In<String>) predicate).transform(this);
    } else if (predicate instanceof Range) {
      // must use a closed range:
      //   if this( abc ) => b then this( acc ) => b, so b must be included
      Range<String> transformed = Ranges.transformClosed(
          (Range<String>) predicate, this);
      return Predicates.in(Ranges.asSet(transformed, domain));
    } else {
      return null;
    }
  }

  @Override
  public Predicate<String> projectStrict(Predicate<String> predicate) {
    if (predicate instanceof Exists) {
      return Predicates.exists();
    } else if (predicate instanceof In) {
      // not possible to check all inputs to the predicate
      return null;
    } else if (predicate instanceof Range) {
      Range<String> transformed = transformClosed((Range<String>) predicate);
      if (transformed != null) {
        return Predicates.in(Ranges.asSet(transformed, domain));
      }
    }
    return null;
  }

  public List<String> getUpperBounds() {
    return upperBounds;
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION",
      justification="Default annotation is not correct for equals")
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || !getClass().equals(o.getClass())) {
      return false;
    }
    RangeFieldPartitioner that = (RangeFieldPartitioner) o;
    return Objects.equal(this.getName(), that.getName()) &&
        Objects.equal(this.upperBounds, that.upperBounds);
  }

  @Override
  public int compare(String o1, String o2) {
    return apply(o1).compareTo(apply(o2));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getName(), upperBounds);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", getName())
        .add("upperBounds", upperBounds).toString();
  }

  /**
   * Transforms a Range predicate to a closed range on this partitioner's upper
   * bounds. Handles edge cases correctly.
   *
   * @param range a Range of Strings
   * @return a Range of upper-bound Strings
   */
  private Range<String> transformClosed(Range<String> range) {
    if (range.hasLowerBound()) {
      String lower = range.lowerEndpoint();
      // the special case, (a, _] and apply(a) == a is handled by skipping a
      String afterLower = domain.next(apply(lower));
      if (afterLower != null) {
        if (range.hasUpperBound()) {
          String upper = range.upperEndpoint();
          String upperImage = apply(upper);
          // meaning: at the endpoint
          if (upper.equals(upperImage) && range.isUpperBoundClosed()) {
            // include upper
            return Ranges.closed(afterLower, upperImage);
          } else {
            String beforeUpper = domain.previous(upperImage);
            if (afterLower.compareTo(beforeUpper) <= 0) {
              return Ranges.closed(afterLower, beforeUpper);
            }
          }
        } else {
          return Ranges.atLeast(afterLower);
        }
      }
    } else if (range.hasUpperBound()) {
      String upper = range.upperEndpoint();
      String upperImage = apply(upper);
      if (upper.equals(upperImage) && range.isUpperBoundClosed()) {
        // include upper
        return Ranges.atMost(upperImage);
      } else {
        String beforeUpper = domain.previous(upperImage);
        if (beforeUpper != null) {
          return Ranges.atMost(beforeUpper);
        }
      }
    }
    return null;
  }

  /**
   * A DiscreteDomain for this partitioner's set of upper bounds.
   *
   * Used for Range#asSet, Predicates.transformClosed,
   * Predicates.excludeEndpoints
   *
   * This DiscreteDomain will throw IllegalArgumentException for values that
   * are after the last upper bound, and will throw IndexOutOfBoundsException
   * when next or previous is called on the last or first bound
   */
  private class RangeDomain extends DiscreteDomain<String> {
    @Override
    public String next(String value) {
      // using apply ensures indexOf returns a real index
      int nextIndex = upperBounds.indexOf(apply(value)) + 1;
      return nextIndex == upperBounds.size() ? null : upperBounds.get(nextIndex);
    }

    @Override
    public String previous(String value) {
      // using apply ensures indexOf returns a real index
      int index = upperBounds.indexOf(apply(value));
      return index == 0 ? null : upperBounds.get(index - 1);
    }

    @Override
    public long distance(String start, String end) {
      // using apply ensures indexOf returns a real index
      return (upperBounds.indexOf(apply(end)) -
          upperBounds.indexOf(apply(start)));
    }

    @Override
    public String minValue() {
      return upperBounds.get(0);
    }

    @Override
    public String maxValue() {
      DiscreteDomains.integers();
      return upperBounds.get(upperBounds.size() - 1);
    }
  }
}
