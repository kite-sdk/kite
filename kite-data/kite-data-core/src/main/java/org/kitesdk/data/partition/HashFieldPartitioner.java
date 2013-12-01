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
package org.kitesdk.data.partition;

import org.kitesdk.data.FieldPartitioner;

import com.google.common.base.Objects;

@edu.umd.cs.findbugs.annotations.SuppressWarnings(value={
        "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE",
        "SE_COMPARATOR_SHOULD_BE_SERIALIZABLE"},
    justification="False positive due to generics.")
public class HashFieldPartitioner extends FieldPartitioner<Object, Integer> {

  public HashFieldPartitioner(String name, int buckets) {
    super(name, Object.class, Integer.class, buckets);
  }

  public HashFieldPartitioner(String sourceName, String name, int buckets) {
    super(sourceName, name, Object.class, Integer.class, buckets);
  }

  @Override
  public Integer apply(Object value) {
    return (value.hashCode() & Integer.MAX_VALUE) % getCardinality();
  }

  @Override
  @Deprecated
  public Integer valueFromString(String stringValue) {
    return Integer.parseInt(stringValue);
  }

  @Override
  public boolean equals(Object o) {
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
