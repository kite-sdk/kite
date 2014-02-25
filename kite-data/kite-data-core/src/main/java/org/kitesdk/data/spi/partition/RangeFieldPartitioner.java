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
import java.util.Arrays;
import java.util.List;
import javax.annotation.concurrent.Immutable;
import org.kitesdk.data.spi.FieldPartitioner;
import com.google.common.base.Objects;
import org.kitesdk.data.spi.Predicates;

@edu.umd.cs.findbugs.annotations.SuppressWarnings(value={
        "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE",
        "SE_COMPARATOR_SHOULD_BE_SERIALIZABLE"},
    justification="False positive due to generics.")
@Immutable
public class RangeFieldPartitioner extends FieldPartitioner<String, String> {

  private final List<String> upperBounds;

  public RangeFieldPartitioner(String name, String... upperBounds) {
    super(name, name, String.class, String.class, upperBounds.length);
    this.upperBounds = Arrays.asList(upperBounds);
  }

  @Override
  public String apply(String value) {
    for (String upper : upperBounds) {
      if (value.compareTo(upper) <= 0) {
        return upper;
      }
    }
    throw new IllegalArgumentException(value + " is outside bounds");
  }

  @Override
  @Deprecated
  public String valueFromString(String stringValue) {
    return stringValue;
  }

  @Override
  public Predicate<String> project(Predicate<String> predicate) {
    if (predicate instanceof Predicates.Exists) {
      return Predicates.exists();
    } else if (predicate instanceof Predicates.In) {
      return ((Predicates.In<String>) predicate).transform(this);
    } else if (predicate instanceof Range) {
      // must use a closed range:
      //   if this( abc ) => b then this( acc ) => b, so b must be included
      return Predicates.transformClosed((Range<String>) predicate, this);
    } else {
      return null;
    }
  }

  public List<String> getUpperBounds() {
    return upperBounds;
  }

  @Override
  public boolean equals(Object o) {
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
}
