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
import javax.annotation.concurrent.Immutable;
import org.kitesdk.data.spi.FieldPartitioner;
import com.google.common.base.Objects;

@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value="SE_COMPARATOR_SHOULD_BE_SERIALIZABLE",
    justification="Implement if we intend to use in Serializable objects "
        + " (e.g., TreeMaps) and use java serialization.")
@Immutable
public class IdentityFieldPartitioner<S extends Comparable> extends FieldPartitioner<S, S> {

  public IdentityFieldPartitioner(String sourceName, String name, Class<S> type,
                                  int buckets) {
    super(sourceName, name, type, type, buckets);
    if (!(type.equals(Integer.class) || type.equals(Long.class) ||
        type.equals(String.class))) {
      throw new IllegalArgumentException("Type not supported " + type);
    }
  }

  @Override
  public S apply(S value) {
    return value;
  }

  @Override
  @Deprecated
  @SuppressWarnings("unchecked")
  public S valueFromString(String stringValue) {
    if (getType() == Integer.class) {
      return (S) Integer.valueOf(stringValue);
    } else if (getType() == Long.class) {
      return (S) Long.valueOf(stringValue);
    } else if (getType() == String.class) {
      return (S) stringValue;
    }
    throw new IllegalArgumentException("Cannot convert string to type " + getType());
  }

  @Override
  public Predicate<S> project(Predicate<S> predicate) {
    return predicate;
  }

  @Override
  public Predicate<S> projectStrict(Predicate<S> predicate) {
    return predicate;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || !getClass().equals(o.getClass())) {
      return false;
    }
    IdentityFieldPartitioner that = (IdentityFieldPartitioner) o;
    return Objects.equal(this.getName(), that.getName()) &&
        Objects.equal(this.getCardinality(), that.getCardinality());
  }

  @Override
  @SuppressWarnings("unchecked")
  public int compare(S o1, S o2) {
    return o1.compareTo(o2);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getName(), getCardinality());
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", getName())
        .add("cardinality", getCardinality()).toString();
  }
}
