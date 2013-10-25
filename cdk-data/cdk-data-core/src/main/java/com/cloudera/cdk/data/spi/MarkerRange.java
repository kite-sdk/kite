/*
 * Copyright 2013 Cloudera.
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

package com.cloudera.cdk.data.spi;

import com.cloudera.cdk.data.Marker;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import javax.annotation.concurrent.Immutable;

/**
 * Represents a range of concrete partitions.
 *
 * Boundaries are defined by {@link Marker} objects, which contain one or more
 * concrete partitions, and can be inclusive or exclusive.
 */
@Immutable
public class MarkerRange {

  private final MarkerComparator comparator;
  private final Boundary start;
  private final Boundary end;

  public MarkerRange(MarkerComparator comparator) {
    this.comparator = comparator;
    this.start = Boundary.UNBOUNDED;
    this.end = Boundary.UNBOUNDED;
  }

  private MarkerRange(
      MarkerComparator comparator,
      Boundary start, Boundary end) {
    this.comparator = comparator;
    this.start = start;
    this.end = end;
  }

  public boolean contains(Object entity) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public boolean contains(Marker marker) {
    return (start.isLessThan(marker) && end.isGreaterThan(marker));
  }

  public MarkerRange from(Marker start) {
    Preconditions.checkArgument(contains(start),
        "Start boundary is outside of this range");

    return new MarkerRange(comparator,
        new Boundary(comparator, start, true), end);
  }

  public MarkerRange fromAfter(Marker start) {
    Preconditions.checkArgument(contains(start),
        "Start boundary is outside of this range");

    return new MarkerRange(comparator,
        new Boundary(comparator, start, false), end);
  }

  public MarkerRange to(Marker end) {
    Preconditions.checkArgument(contains(end),
        "End boundary is outside of this range");

    return new MarkerRange(comparator, start,
        new Boundary(comparator, end, true));
  }

  public MarkerRange toBefore(Marker end) {
    Preconditions.checkArgument(contains(end),
        "End boundary is outside of this range");

    return new MarkerRange(comparator, start,
        new Boundary(comparator, end, false));
  }

  public MarkerRange in(Marker partial) {
    Preconditions.checkArgument(contains(partial),
        "Marker is outside of this range");
    return new MarkerRange(comparator,
        new Boundary(comparator, partial, true),
        new Boundary(comparator, partial, true));
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("start", start)
        .add("end", end)
        .toString();
  }

  /**
   * Represents the boundary of a range, either inclusive or exclusive.
   */
  static class Boundary {

    public static final Boundary UNBOUNDED = new Boundary();

    private final MarkerComparator comparator;
    private final Marker bound;
    private final boolean isInclusive;

    private Boundary() {
      this.comparator = null;
      this.bound = null;
      this.isInclusive = true;
    }

    public Boundary(MarkerComparator comparator, Marker bound, boolean isInclusive) {
      parquet.Preconditions.checkArgument(comparator != null,
          "Comparator cannot be null");
      parquet.Preconditions.checkArgument(bound != null,
          "Bound cannot be null");

      this.comparator = comparator;
      this.bound = bound;
      this.isInclusive = isInclusive;
    }

    public boolean isLessThan(Marker other) {
      if (comparator == null) {
        return true;
      }

      if (isInclusive) {
        // compare left-most side
        return (comparator.leftCompare(bound, other) <= 0);
      } else {
        try {
          return (comparator.compare(bound, other) < 0);
        } catch (IllegalStateException ex) {
          // one contained the other, which is not allowed for exclusive ranges
          return false;
        }
      }
    }

    public boolean isGreaterThan(Marker other) {
      if (comparator == null) {
        return true;
      }

      if (isInclusive) {
        // compare the right-most side
        return (comparator.rightCompare(bound, other) >= 0);
      } else {
        try {
          return (comparator.compare(bound, other) > 0);
        } catch (IllegalStateException ex) {
          // one contained the other, which is not allowed for exclusive ranges
          return false;
        }
      }
    }

    @Override
    public String toString() {
      if (comparator == null) {
        return Objects.toStringHelper(this)
          .add("bound", "UNBOUNDED")
          .toString();
      } else {
        return Objects.toStringHelper(this)
            .add("inclusive", isInclusive)
            .add("bound", bound)
            .toString();
      }
    }
  }
}
