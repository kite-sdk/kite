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

import java.util.List;
import java.util.Set;

import com.cloudera.data.FieldPartitioner;
import com.google.common.base.Objects;

public class ListFieldPartitioner extends FieldPartitioner {

  private List<Set<?>> values;

  public ListFieldPartitioner(String name, List<Set<?>> values) {
    super(name, cardinality(values));
    this.values = values;
  }

  private static int cardinality(List<Set<?>> values) {
    int c = 0;
    for (Set<?> set : values) {
      c += set.size();
    }
    return c;
  }

  @Override
  public Object apply(Object value) {
    for (int i = 0; i < values.size(); i++) {
      if (values.get(i).contains(value)) {
        return i;
      }
    }

    throw new IllegalArgumentException(value + " is not in set");
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", getName())
        .add("values", values).toString();
  }
}
