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
import com.google.common.collect.DiscreteDomains;
import com.google.common.base.Objects;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.predicates.Exists;
import org.kitesdk.data.spi.predicates.In;
import org.kitesdk.data.spi.predicates.Predicates;
import org.kitesdk.data.spi.predicates.Range;
import org.kitesdk.data.spi.predicates.Ranges;

@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value="SE_COMPARATOR_SHOULD_BE_SERIALIZABLE",
    justification="Implement if we intend to use in Serializable objects "
        + " (e.g., TreeMaps) and use java serialization.")
@Immutable
public class IntRangeFieldPartitioner extends FieldPartitioner<Integer, Integer> {

  private final int[] upperBounds;

  public IntRangeFieldPartitioner(String sourceName, int... upperBounds) {
    this(sourceName, null, upperBounds);
  }

  public IntRangeFieldPartitioner(String sourceName, @Nullable String name,
                                  int... upperBounds) {
    super(sourceName, (name == null ? sourceName + "_bound" : name),
        Integer.class, Integer.class, upperBounds.length);
    this.upperBounds = upperBounds;
  }

  @Override
  public Integer apply(Integer value) {
    for (int i = 0; i < upperBounds.length; i++) {
      if (value <= upperBounds[i]) {
        return i;
      }
    }

    throw new IllegalArgumentException(value + " is outside bounds");
  }

  @Override
  @Deprecated
  public Integer valueFromString(String stringValue) {
    return Integer.parseInt(stringValue);
  }

  @Override
  public Predicate<Integer> project(Predicate<Integer> predicate) {
    if (predicate instanceof Exists) {
      return Predicates.exists();
    } else if (predicate instanceof In) {
      return ((In<Integer>) predicate).transform(this);
    } else if (predicate instanceof Range) {
      // must use a closed range:
      //   if this( 5 ) => 10 then this( 6 ) => 10, so 10 must be included
      return Ranges.transformClosed(
          Ranges.adjustClosed((Range<Integer>) predicate,
              DiscreteDomains.integers()), this);
    } else {
      return null;
    }
  }

  @Override
  public Predicate<Integer> projectStrict(Predicate<Integer> predicate) {
    if (predicate instanceof Exists) {
      return Predicates.exists();

    } else if (predicate instanceof In) {
      // accumulate the bounds for which all inputs match the predicate
      Set<Integer> possibleValues = Sets.newHashSet();
      int end = upperBounds[upperBounds.length - 1];
      int nextIndex = 1; // the first bound is always excluded
      boolean matchedAll = true;
      for (int i = upperBounds[0] + 1; i <= end; i += 1) {
        matchedAll = matchedAll && predicate.apply(i);
        // if at an upper bound, see if it should be added and reset
        if (i == upperBounds[nextIndex]) {
          if (matchedAll) {
            possibleValues.add(nextIndex);
          }
          // reset for the next bound
          matchedAll = true;
          nextIndex += 1;
        }
      }
      if (!possibleValues.isEmpty()) {
        return Predicates.in(possibleValues);
      }

    } else if (predicate instanceof Range) {
      Range<Integer> adjusted = Ranges.adjustClosed(
          (Range<Integer>) predicate, DiscreteDomains.integers());
      if (adjusted.hasLowerBound()) {
        int lower = adjusted.lowerEndpoint();
        int lowerIndex = apply(lower);
        if ((lowerIndex == 0) || (upperBounds[lowerIndex - 1] < lower - 1)) {
          // at least one value that maps to lowerIndex but is not included
          lowerIndex += 1;
        }
        if (adjusted.hasUpperBound()) {
          int upper = adjusted.upperEndpoint();
          int upperIndex = apply(upper);
          if (upperBounds[upperIndex] > upper + 1) {
            // at least one value that maps to upperIndex but is not included
            upperIndex -= 1;
          }
          if (lowerIndex <= upperIndex) {
            return Ranges.closed(lowerIndex, upperIndex);
          }
        } else {
          // while this includes values for which this function will throw
          // IllegalArgumentException, this is a fair projection of the
          // predicate. this is used assuming that apply succeeded.
          return Ranges.atLeast(lowerIndex);
        }
      } else if (adjusted.hasUpperBound()) {
        int upper = adjusted.upperEndpoint();
        int upperIndex = apply(upper);
        if (upperBounds[upperIndex] > upper + 1) {
          // at least one value that maps to upperIndex but is not included
          upperIndex -= 1;
        }
        return Ranges.atMost(upperIndex);
      }
    }
    return null;
  }

  int[] getUpperBounds() {
    return upperBounds;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || !getClass().equals(o.getClass())) {
      return false;
    }
    IntRangeFieldPartitioner that = (IntRangeFieldPartitioner) o;
    return Objects.equal(this.getName(), that.getName()) &&
        Objects.equal(Ints.asList(this.upperBounds), Ints.asList(that.upperBounds));
  }

  @Override
  public int compare(Integer o1, Integer o2) {
    return apply(o1).compareTo(apply(o2));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getName(), Ints.asList(upperBounds));
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", getName())
      .add("upperBounds", Ints.asList(upperBounds)).toString();
  }
}
