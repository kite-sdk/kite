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

import org.kitesdk.data.PartitionStrategy;
import java.util.Comparator;
import javax.annotation.concurrent.Immutable;
import org.kitesdk.data.impl.Accessor;

/**
 * Comparison methods for {@link Marker} objects with respect to a
 * {@link PartitionStrategy}.
 *
 * @since 0.9.0
 */
@Immutable
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value="SE_COMPARATOR_SHOULD_BE_SERIALIZABLE",
    justification="Implement if we intend to use in Serializable objects "
        + " (e.g., TreeMaps) and use java serialization.")
public class MarkerComparator implements Comparator<Marker> {
  private final PartitionStrategy strategy;

  public MarkerComparator(PartitionStrategy strategy) {
    this.strategy = strategy;
  }

  /**
   * Returns true if {@code container} contains {@code test}, false otherwise.
   *
   * Fields for which {@code set} has no value
   * ({@link Marker#valueFor(FieldPartitioner)} returns
   * null) are treated as wildcards and always match.
   *
   * All fields in the {@link PartitionStrategy} are compared.
   *
   * @param container a {@code Marker} that defines a set of partitions
   * @param test a {@code Marker} that may be a subset of {@code container}
   * @return
   *      {@code true} if the partitions in {@code test} are a subset of
   *      {@code container}, {@code false} otherwise
   */
  @SuppressWarnings("unchecked")
  public boolean contains(Marker container, Marker test) {
    for (FieldPartitioner field : Accessor.getDefault().getFieldPartitioners(strategy)) {
      Object containerValue = container.valueFor(field);
      if (containerValue != null) {
        Object testValue = test.valueFor(field);
        if (testValue == null || field.compare(containerValue, testValue) != 0) {
          return false;
        }
      }
      /*
       * Rather than returning true if containerValue is null, this treats
       * null as a wildcard. Everything matches null, so all non-null fields
       * will be checked.
       */
    }
    return true;
  }

  /**
   * Compare two {@link Marker} objects under the {@link PartitionStrategy}.
   *
   * All comparisons are with respect to the partition ordering defined by
   * this comparator's {@code PartitionStrategy}. Under a
   * {@code PartitionStrategy}, a {@code Marker} contains a set of one or
   * more partitions. A {@code Marker} is strictly less than another if all of
   * the partitions it contains are less than the partitions of the other.
   * Similarly, if all partitions are greater than the partitions of the other,
   * then the {@code Marker} is greater. Two {@code Markers} are equal if they
   * contain the same set of partitions.
   *
   * This method implements strictly exclusive comparison: if either
   * {@code Marker} contains the other, then this throws
   * {@code IllegalStateException}. This is because there is at least one
   * partition in the containing {@code Marker} that is less than or equal to
   * all partitions in the contained {@code Marker} and at least one partition
   * that is greater than or equal to all partitions in the contained
   * {@code Marker}.
   *
   * Alternatively, the comparison methods {@link #leftCompare(Marker, Marker)}
   * and {@link #rightCompare(Marker, Marker)} consider contained {@code Marker}
   * objects to be greater-than and less-than respectively.
   *
   * Note: Because {@code Marker} objects are hierarchical, they are either
   * completely disjoint or one marker contains the other. If one contains the
   * other and the two are not equal, this method throws
   * {@code IllegalStateException}.
   *
   * TODO: catch wildcard to concrete comparisons and throw an Exception
   *
   * @param m1 a {@code Marker}
   * @param m2 a {@code Marker}
   * @return
   *      -1 If all partitions in m1 are less than the partitions in m2
   *       0 If m1 and m2 contain the same set of partitions
   *       1 If all partitions of m1 are greater than the partitions in m2
   * @throws IllegalStateException
   *      If either {@code Marker} is a proper subset of the other
   *
   * @see MarkerComparator#leftCompare(Marker, Marker)
   * @see MarkerComparator#rightCompare(Marker, Marker)
   *
   * @since 0.9.0
   */
  @Override
  @SuppressWarnings("unchecked")
  public int compare(Marker m1, Marker m2) {
    for (FieldPartitioner field : Accessor.getDefault().getFieldPartitioners(strategy)) {
      Object m1Value = m1.valueFor(field);
      Object m2Value = m2.valueFor(field);
      // if either is null, but not both, then they are Incomparable
      if (m1Value == null) {
        if (m2Value != null) {
          // m1 contains m2
          throw new IllegalStateException("Incomparable");
        }
      } else if (m2Value == null) {
        // m2 contains m1
        throw new IllegalStateException("Incomparable");
      } else {
        int cmp = field.compare(m1Value, m2Value);
        if (cmp != 0) {
          return cmp;
        }
      }
    }
    return 0;
  }

  /**
   * Compare two {@link Marker} objects under the {@link PartitionStrategy}.
   *
   * All comparisons are with respect to the partition ordering defined by
   * this comparator's {@code PartitionStrategy}. Under a
   * {@code PartitionStrategy}, a {@code Marker} contains a set of one or
   * more partitions. A {@code Marker} is strictly less than another if all of
   * the partitions it contains are less than the partitions of the other.
   * Similarly, if all partitions are greater than the partitions of the other,
   * then the {@code Marker} is greater. Two {@code Markers} are equal if they
   * contain the same set of partitions.
   *
   * This method implements right-inclusive comparison: if either {@code Marker}
   * contains the other, then it is considered greater. This means that there
   * is at least one partition in the containing {@code Marker} that is greater
   * than all of the partitions in the contained {@code Marker}. This behavior
   * is for checking an inclusive upper bound for a range.
   *
   * m1 = [ 2013, Oct, * ]
   * m2 = [ 2013, Oct, 12 ]
   * rightCompare(m1, m2) returns 1
   * rightCompare(m2, m1) returns -1
   *
   * The comparison method {@link #leftCompare(Marker, Marker)} implements
   * left-inclusive comparison.
   *
   * Note: Because {@code Marker} objects are hierarchical, they are either
   * completely disjoint or one marker contains the other. If one contains the
   * other and the two are not equal, this method considers it to be greater
   * than the other.
   *
   * TODO: catch wildcard to concrete comparisons and throw an Exception
   *
   * @param m1 a {@code Marker}
   * @param m2 a {@code Marker}
   * @return
   *      -1 If all partitions in m1 are less than the partitions in m2
   *       0 If m1 and m2 contain the same set of partitions
   *       1 If all partitions of m1 are greater than the partitions in m2
   *
   * @see MarkerComparator#compare(Marker, Marker)
   * @see MarkerComparator#leftCompare(Marker, Marker)
   *
   * @since 0.9.0
   */
  @SuppressWarnings("unchecked")
  public int rightCompare(Marker m1, Marker m2) {
    for (FieldPartitioner field : Accessor.getDefault().getFieldPartitioners(strategy)) {
      Object m1Value = m1.valueFor(field);
      Object m2Value = m2.valueFor(field);
      if (m1Value == null) {
        if (m2Value != null) {
          // m1 contains m2
          return 1;
        }
      } else if (m2Value == null) {
        // m2 contains m1
        return -1;
      } else {
        int cmp = field.compare(m1Value, m2Value);
        if (cmp != 0) {
          return cmp;
        }
      }
    }
    return 0;
  }

  /**
   * Compare two {@link Marker} objects under the {@link PartitionStrategy}.
   *
   * All comparisons are with respect to the partition ordering defined by
   * this comparator's {@code PartitionStrategy}. Under a
   * {@code PartitionStrategy}, a {@code Marker} contains a set of one or
   * more partitions. A {@code Marker} is strictly less than another if all of
   * the partitions it contains are less than the partitions of the other.
   * Similarly, if all partitions are greater than the partitions of the other,
   * then the {@code Marker} is greater. Two {@code Markers} are equal if they
   * contain the same set of partitions.
   *
   * This method implements left-inclusive comparison: if either {@code Marker}
   * contains the other, then it is considered lesser. This means that there
   * is at least one partition in the containing {@code Marker} that is less
   * than all of the partitions in the contained {@code Marker}. This behavior
   * is for checking an inclusive lower bound for a range.
   *
   * m1 = [ 2013, Oct, * ]
   * m2 = [ 2013, Oct, 12 ]
   * leftCompare(m1, m2) returns 1
   * leftCompare(m2, m1) returns -1
   *
   * The comparison method {@link #rightCompare(Marker, Marker)} implements
   * right-inclusive comparison.
   *
   * Note: Because {@code Marker} objects are hierarchical, they are either
   * completely disjoint or one marker contains the other. If one contains the
   * other and the two are not equal, this method considers it to be less than
   * than the other.
   *
   * TODO: catch wildcard to concrete comparisons and throw an Exception
   *
   * @param m1 a {@code Marker}
   * @param m2 a {@code Marker}
   * @return
   *      -1 If all partitions in m1 are less than the partitions in m2
   *       0 If m1 and m2 contain the same set of partitions
   *       1 If all partitions of m1 are greater than the partitions in m2
   *
   * @see MarkerComparator#compare(Marker, Marker)
   * @see MarkerComparator#rightCompare(Marker, Marker)
   *
   * @since 0.9.0
   */
  @SuppressWarnings("unchecked")
  public int leftCompare(Marker m1, Marker m2) {
    for (FieldPartitioner field : Accessor.getDefault().getFieldPartitioners(strategy)) {
      Object m1Value = m1.valueFor(field);
      Object m2Value = m2.valueFor(field);
      if (m1Value == null) {
        if (m2Value != null) {
          // m1 contains m2
          return -1;
        }
      } else if (m2Value == null) {
        // m2 contains m1
        return 1;
      } else {
        int cmp = field.compare(m1Value, m2Value);
        if (cmp != 0) {
          return cmp;
        }
      }
    }
    return 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    MarkerComparator that = (MarkerComparator) o;

    return strategy.equals(that.strategy);
  }

  @Override
  public int hashCode() {
    return strategy.hashCode();
  }
}
