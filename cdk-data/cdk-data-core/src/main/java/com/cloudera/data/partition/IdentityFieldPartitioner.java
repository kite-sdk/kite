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
package com.cloudera.data.partition;

import com.cloudera.data.FieldPartitioner;
import com.google.common.annotations.Beta;
import com.google.common.base.Objects;

@Beta
public class IdentityFieldPartitioner extends FieldPartitioner {

  public IdentityFieldPartitioner(String name, int buckets) {
    super(name, buckets);
  }

  @Override
  public Object apply(Object value) {
    return value;
  }

  @Override
  public Object valueFromString(String stringValue) {
    return stringValue; // TODO: need more type information
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
