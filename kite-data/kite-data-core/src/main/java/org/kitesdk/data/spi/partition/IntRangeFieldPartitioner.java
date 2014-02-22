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
import com.google.common.collect.Range;
import org.kitesdk.data.spi.FieldPartitioner;
import com.google.common.annotations.Beta;
import com.google.common.base.Objects;

import com.google.common.primitives.Ints;
import org.kitesdk.data.spi.Predicates;

@Beta
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value="SE_COMPARATOR_SHOULD_BE_SERIALIZABLE",
    justification="Implement if we intend to use in Serializable objects "
        + " (e.g., TreeMaps) and use java serialization.")
public class IntRangeFieldPartitioner extends FieldPartitioner<Integer, Integer> {

  private final int[] upperBounds;

  public IntRangeFieldPartitioner(String name, int... upperBounds) {
    super(name, name, Integer.class, Integer.class, upperBounds.length);
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
    if (predicate instanceof Predicates.Exists) {
      return Predicates.exists();
    } else if (predicate instanceof Predicates.In) {
      return ((Predicates.In<Integer>) predicate).transform(this);
    } else if (predicate instanceof Range) {
      // must use a closed range:
      //   if this( 5 ) => 10 then this( 6 ) => 10, so 10 must be included
      return Predicates.transformClosed((Range<Integer>) predicate, this);
    } else {
      return null;
    }
  }

  @Override
  public boolean equals(Object o) {
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
