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
import com.google.common.collect.Sets;
import java.util.Set;
import java.util.TreeSet;
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
public class LongFixedSizeRangeFieldPartitioner extends FieldPartitioner<Long, Long> {

  private final long size;

  public LongFixedSizeRangeFieldPartitioner(String sourceName, long size) {
    this(sourceName, null, size);
  }

  public LongFixedSizeRangeFieldPartitioner(String sourceName, @Nullable String name,
      long size) {
    super(sourceName, (name == null ? sourceName + "_range" : name),
        Long.class, Long.class);
    this.size = size;
    Preconditions.checkArgument(size > 0,
        "Size of range buckets is not positive: %s", size);
  }

  @Override
  public Long apply(Long value) {
    return Math.round(Math.floor(value / ((double) size))) * size;
  }

  @Override
  public Predicate<Long> project(Predicate<Long> predicate) {
    if (predicate instanceof Exists) {
      return Predicates.exists();
    } else if (predicate instanceof In) {
      return ((In<Long>) predicate).transform(this);
    } else if (predicate instanceof Range) {
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
      Set<Long> possibleValues = Sets.newHashSet();
      In<Long> in = ((In<Long>) predicate).transform(this);
      for (Long val : Predicates.asSet(in)) {
        boolean matchedAll = true;
        for (long i = 0; i < size; i++) {
          matchedAll = matchedAll && predicate.apply(val + i);
        }
        if (matchedAll) {
          possibleValues.add(val);
        }
      }
      if (!possibleValues.isEmpty()) {
        return Predicates.in(possibleValues);
      }
    } else if (predicate instanceof Range) {
      Range<Long> closed = Ranges.adjustClosed(
          (Range<Long>) predicate, DiscreteDomains.longs());
      Long start = null;
      if (closed.hasLowerBound()) {
        if ((closed.lowerEndpoint() % size) == 0) {
          // the entire set of values is included
          start = closed.lowerEndpoint();
        } else {
          // start the predicate at the next value
          start = apply(closed.lowerEndpoint() + size);
        }
      }
      Long end = null;
      if (closed.hasUpperBound()) {
        if (((closed.upperEndpoint() + 1) % size) == 0) {
          // all values are included
          end = apply(closed.upperEndpoint());
        } else {
          // end the predicate at the previous value
          end = apply(closed.upperEndpoint() - size);
        }
      }
      if (start != null && end != null && start > end) {
        return null;
      }
      return Ranges.closed(start, end); // null start or end => unbound
    }
    return null;
  }

  public long getSize() {
    return size;
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
    LongFixedSizeRangeFieldPartitioner that = (LongFixedSizeRangeFieldPartitioner) o;
    return Objects.equal(this.getName(), that.getName()) &&
        Objects.equal(this.size, that.size);
  }

  @Override
  public int compare(Long o1, Long o2) {
    return apply(o1).compareTo(apply(o2));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getName(), size);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", getName())
      .add("size", size).toString();
  }
}
