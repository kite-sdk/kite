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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.kitesdk.data.spi.FieldPartitioner;

import com.google.common.base.Objects;
import org.kitesdk.data.spi.predicates.Exists;
import org.kitesdk.data.spi.predicates.In;
import org.kitesdk.data.spi.predicates.Predicates;

@edu.umd.cs.findbugs.annotations.SuppressWarnings(value={
        "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE",
        "SE_COMPARATOR_SHOULD_BE_SERIALIZABLE"},
    justification="False positive due to generics.")
@Immutable
public class HashFieldPartitioner extends FieldPartitioner<Object, Integer> {

  public HashFieldPartitioner(String sourceName, int buckets) {
    // create a new field name from the source name
    this(sourceName, null, buckets);
  }

  public HashFieldPartitioner(String sourceName, @Nullable String name,
                              int buckets) {
    super(sourceName, (name == null ? sourceName + "_hash" : name),
        Object.class, Integer.class, buckets);
    Preconditions.checkArgument(buckets > 0,
        "Number of hash buckets is negative: %s", buckets);
  }

  @Override
  public Integer apply(Object value) {
    return (value.hashCode() & Integer.MAX_VALUE) % getCardinality();
  }

  @Override
  public Predicate<Integer> project(Predicate<Object> predicate) {
    if (predicate instanceof Exists) {
      return Predicates.exists();
    } else if (predicate instanceof In) {
      return ((In<Object>) predicate).transform(this);
    } else {
      // cannot enumerate ranges and get the image
      return null;
    }
  }

  @Override
  public Predicate<Integer> projectStrict(Predicate<Object> predicate) {
    return null;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || !getClass().equals(o.getClass())) {
      return false;
    }
    HashFieldPartitioner that = (HashFieldPartitioner) o;
    return Objects.equal(this.getSourceName(), that.getSourceName()) &&
        Objects.equal(this.getName(), that.getName()) &&
        Objects.equal(this.getCardinality(), that.getCardinality());
  }

  @Override
  public int compare(Integer o1, Integer o2) {
    return o1.compareTo(o2);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getSourceName(), getName(), getCardinality());
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("sourceName", getSourceName())
        .add("name", getName())
        .add("cardinality", getCardinality()).toString();
  }
}
