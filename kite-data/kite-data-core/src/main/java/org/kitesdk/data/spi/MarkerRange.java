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

package org.kitesdk.data.spi;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import java.io.Serializable;
import java.util.Comparator;
import javax.annotation.concurrent.Immutable;

/**
 * Represents a range of concrete partitions.
 *
 * Boundaries are defined by {@link Marker} objects, which contain one or more
 * concrete partitions, and can be inclusive or exclusive.
 *
 * @since 0.9.0
 */
@Immutable
public class MarkerRange {

  private final MarkerComparator comparator;
  private final Boundary start;
  private final Boundary end;

  private MarkerRange() {
    this.comparator = null;
    this.start = Boundary.NEGATIVE_INFINITY;
    this.end = Boundary.POSITIVE_INFINITY;
  }

  public MarkerRange(MarkerComparator comparator) {
    Preconditions.checkArgument(comparator != null,
        "Comparator cannot be null.");

    this.comparator = comparator;
    this.start = Boundary.NEGATIVE_INFINITY;
    this.end = Boundary.POSITIVE_INFINITY;
  }

  private MarkerRange(
      MarkerComparator comparator,
      Boundary start, Boundary end) {
    try {
      if (start.compareTo(end) > 0) {
        throw new IllegalArgumentException("Invalid range: " + start + ", " + end);
      }
    } catch (IllegalStateException e) {
      // one boundary contains the other
      // check that the containing boundary is an inclusive bound
      if ((comparator.contains(start.bound, end.bound) && !start.isInclusive) ||
          (comparator.contains(end.bound, start.bound) && !end.isInclusive)) {
        throw new IllegalArgumentException("Invalid range: " + start + ", " + end);
      }
    }
    this.comparator = comparator;
    this.start = start;
    this.end = end;
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

  public MarkerRange of(Marker partial) {
    Preconditions.checkArgument(contains(partial),
        "Marker is outside of this range");
    return new MarkerRange(comparator,
        new Boundary(comparator, partial, true),
        new Boundary(comparator, partial, true));
  }

  public MarkerRange intersection(MarkerRange other) {
    Boundary newStart = Ordering.from(new Boundary.LeftComparator()).max(start, other.start);
    Boundary newEnd = Ordering.from(new Boundary.RightComparator()).min(end, other.end);
    return new MarkerRange(comparator, newStart, newEnd);
  }

  public MarkerRange span(MarkerRange other) {
    Boundary newStart = Ordering.from(new Boundary.LeftComparator()).min(start, other.start);
    Boundary newEnd = Ordering.from(new Boundary.RightComparator()).max(end, other.end);
    return new MarkerRange(comparator, newStart, newEnd);
  }

  public MarkerRange complement() {
    if (start == Boundary.NEGATIVE_INFINITY && end == Boundary.POSITIVE_INFINITY) {
      throw new IllegalArgumentException("Cannot find complement of unbounded range.");
    }
    if (start == Boundary.NEGATIVE_INFINITY) {
      Boundary newStart = new Boundary(comparator, end.bound, !end.isInclusive);
      Boundary newEnd = Boundary.POSITIVE_INFINITY;
      return new MarkerRange(comparator, newStart, newEnd);
    } else if (end == Boundary.POSITIVE_INFINITY) {
      Boundary newStart = Boundary.NEGATIVE_INFINITY;
      Boundary newEnd = new Boundary(comparator, start.bound, !start.isInclusive);
      return new MarkerRange(comparator, newStart, newEnd);
    }
    return new MarkerRange(); // unbounded TODO: use set of ranges to improve efficiency
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      // this covers the UNDEFINED case
      return true;
    }

    if (!(o instanceof MarkerRange)) {
      return false;
    }

    MarkerRange that = (MarkerRange) o;
    return (Objects.equal(this.start, that.start) &&
        Objects.equal(this.end, that.end));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(start, end);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("start", start)
        .add("end", end)
        .toString();
  }

  public Boundary getStart() {
    return start;
  }

  public Boundary getEnd() {
    return end;
  }

  /**
   * Represents the boundary of a range, either inclusive or exclusive.
   *
   * @since 0.9.0
   */
  public static class Boundary implements Comparable<Boundary> {

    private final MarkerComparator comparator;
    private final Marker bound;
    private final boolean isInclusive;

    private Boundary() {
      this.comparator = null;
      this.bound = null;
      this.isInclusive = false;
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
    public int compareTo(Boundary other) {
      if (this == NEGATIVE_INFINITY) {
        return other == NEGATIVE_INFINITY ? 0 : -1;
      } else if (other == NEGATIVE_INFINITY) {
        return 1;
      } else if (this == POSITIVE_INFINITY) {
        return other == POSITIVE_INFINITY ? 0 : 1;
      } else if (other == POSITIVE_INFINITY) {
        return -1;
      }
      return comparator.compare(bound, other.bound);
    }

    public Marker getBound() {
      return bound;
    }

    public boolean isInclusive() {
      return isInclusive;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof Boundary)) {
        return false;
      }

      Boundary that = (Boundary) o;
      return (Objects.equal(this.isInclusive, that.isInclusive) &&
          Objects.equal(this.bound, that.bound) &&
          Objects.equal(this.comparator, that.comparator));
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(isInclusive, bound, comparator);
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
            .add("comparator", comparator)
            .toString();
      }
    }

    public static final Boundary NEGATIVE_INFINITY = new Boundary() {
      @Override
      public boolean isLessThan(Marker other) {
        return true;
      }

      @Override
      public boolean isGreaterThan(Marker other) {
        return false;
      }

      @Override
      public boolean equals(Object o) {
        return this == o;
      }

      @Override
      public String toString() {
        return Objects.toStringHelper(this)
              .add("bound", "NEGATIVE_INFINITY")
              .toString();
      }
    };

    public static final Boundary POSITIVE_INFINITY = new Boundary() {
      @Override
      public boolean isLessThan(Marker other) {
        return false;
      }

      @Override
      public boolean isGreaterThan(Marker other) {
        return true;
      }

      @Override
      public boolean equals(Object o) {
        return this == o;
      }

      @Override
      public String toString() {
        return Objects.toStringHelper(this)
            .add("bound", "POSITIVE_INFINITY")
            .toString();
      }
    };

    public static class LeftComparator implements Comparator<Boundary>, Serializable {
      private static final long serialVersionUID = 0;
      @Override
      public int compare(Boundary b1, Boundary b2) {
        if (b1 == NEGATIVE_INFINITY) {
          return b2 == NEGATIVE_INFINITY ? 0 : -1;
        } else if (b2 == NEGATIVE_INFINITY) {
          return 1;
        } else if (b1 == POSITIVE_INFINITY) {
          return b2 == POSITIVE_INFINITY ? 0 : 1;
        } else if (b2 == POSITIVE_INFINITY) {
          return -1;
        }
        if (b1.isInclusive) {
          return b1.comparator.leftCompare(b1.bound, b2.bound);
        } else {
          try {
            return b1.comparator.compare(b1.bound, b2.bound);
          } catch (IllegalStateException ex) {
            // one contained the other, which is not allowed for exclusive ranges
            return 0;
          }
        }
      }
    }

    public static class RightComparator implements Comparator<Boundary>, Serializable {
      private static final long serialVersionUID = 0;
      @Override
      public int compare(Boundary b1, Boundary b2) {
        if (b1 == NEGATIVE_INFINITY) {
          return b2 == NEGATIVE_INFINITY ? 0 : -1;
        } else if (b2 == NEGATIVE_INFINITY) {
          return 1;
        } else if (b1 == POSITIVE_INFINITY) {
          return b2 == POSITIVE_INFINITY ? 0 : 1;
        } else if (b2 == POSITIVE_INFINITY) {
          return -1;
        }
        if (b1.isInclusive) {
          return b1.comparator.rightCompare(b1.bound, b2.bound);
        } else {
          try {
            return b1.comparator.compare(b1.bound, b2.bound);
          } catch (IllegalStateException ex) {
            // one contained the other, which is not allowed for exclusive ranges
            return 0;
          }
        }

      }
    }

  }

  public static class Builder {

    private Marker.Builder start;
    private Marker.Builder end;
    private MarkerComparator comparator;

    public Builder(MarkerComparator comparator) {
      this.comparator = comparator;
    }

    public Builder addToStart(String name, Object value) {
      if (start == null) {
        start = new Marker.Builder();
      }
      start.add(name, value);
      return this;
    }

    public Builder addToEnd(String name, Object value) {
      if (end == null) {
        end = new Marker.Builder();
      }
      end.add(name, value);
      return this;
    }

    public MarkerRange build() {
      MarkerRange markerRange = new MarkerRange(comparator);
      if (start != null) {
        markerRange = markerRange.from(start.build());
      }
      if (end != null) {
        markerRange = markerRange.to(end.build());
      }
      return markerRange;
    }
  }
}
