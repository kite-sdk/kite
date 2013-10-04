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
package com.cloudera.cdk.data.partition;

import com.cloudera.cdk.data.FieldPartitioner;
import com.google.common.annotations.Beta;
import com.google.common.base.Objects;

@Beta
public class IdentityFieldPartitioner<S> extends FieldPartitioner<S, S> {

  public IdentityFieldPartitioner(String name, Class<S> type, int buckets) {
    super(name, type, buckets);
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
  public int hashCode() {
    return Objects.hashCode(getName(), getCardinality());
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", getName())
        .add("cardinality", getCardinality()).toString();
  }
}
