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

import edu.umd.cs.findbugs.annotations.SuppressWarnings;
import java.util.Arrays;
import java.util.List;

import com.cloudera.cdk.data.FieldPartitioner;
import com.google.common.annotations.Beta;
import com.google.common.base.Objects;

@Beta
@SuppressWarnings(value="NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE",
    justification="False positive due to generics.")
public class RangeFieldPartitioner extends FieldPartitioner<String, String> {

  private final List<String> upperBounds;

  public RangeFieldPartitioner(String name, String... upperBounds) {
    super(name, String.class, upperBounds.length);
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
  public String valueFromString(String stringValue) {
    return stringValue;
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
  public int hashCode() {
    return Objects.hashCode(getName(), upperBounds);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", getName())
        .add("upperBounds", upperBounds).toString();
  }
}
