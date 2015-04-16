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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.DiscreteDomains;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.kitesdk.data.spi.FieldPartitioner;
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
public class FixedLongRangeFieldPartitioner extends FieldPartitioner<Long, Long> {

  private final long range;

  public FixedLongRangeFieldPartitioner(String sourceName, long range) {
    this(sourceName, null, range);
  }

  public FixedLongRangeFieldPartitioner(String sourceName, @Nullable String name,
      long range) {
    super(sourceName, (name == null ? sourceName + "_range" : name),
        Long.class, Long.class);
    this.range = range;
    Preconditions.checkArgument(range > 0,
        "Size of range buckets is not positive: %s", range);
  }

  @Override
  public Long apply(Long value) {
    return Math.round(Math.floor(value/((double) range)));
  }

  @Override
  public Predicate<Long> project(Predicate<Long> predicate) {
    if (predicate instanceof Exists) {
      return Predicates.exists();
    } else if (predicate instanceof In) {
      return ((In<Long>) predicate).transform(this);
    } else if (predicate instanceof Range) {
      // must use a closed range:
      //   if this( 5 ) => 10 then this( 6 ) => 10, so 10 must be included
      return Ranges.transformClosed(
          Ranges.adjustClosed((Range<Long>) predicate,
              DiscreteDomains.longs()), this);
    } else {
      return null;
    }
  }

  @Override
  public Predicate<Long> projectStrict(Predicate<Long> predicate) {
    if (predicate instanceof Exists) {
      return Predicates.exists();
    } else if (predicate instanceof In) {
      return ((In<Long>) predicate).transform(this);
    }
    return null;
  }

  public long getRange() {
    return range;
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
    FixedLongRangeFieldPartitioner that = (FixedLongRangeFieldPartitioner) o;
    return Objects.equal(this.getName(), that.getName()) &&
        Objects.equal(this.range, that.range);
  }

  @Override
  public int compare(Long o1, Long o2) {
    return apply(o1).compareTo(apply(o2));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getName(), range);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", getName())
      .add("range", range).toString();
  }
}
