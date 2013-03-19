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
package com.cloudera.data;

import com.google.common.base.Objects;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.concurrent.Immutable;

/**
 * <p>
 * A key for retrieving partitions from a {@link Dataset}.
 * </p>
 * <p>
 * A {@code PartitionKey} is a ordered sequence of values corresponding to the
 * {@link FieldPartitioner}s in a {@link PartitionStrategy}. A
 * {@link PartitionKey} may be obtained using
 * {@link PartitionStrategy#partitionKey(Object...)} or
 * {@link PartitionStrategy#partitionKeyForEntity(Object)}.
 * </p>
 * 
 * @see PartitionStrategy
 * @see FieldPartitioner
 * @see Dataset
 */
@Immutable
public class PartitionKey {

  private final List<Object> values;

  PartitionKey(Object... values) {
    this.values = Arrays.asList(values);
  }

  public List<Object> getValues() {
    return Collections.unmodifiableList(values);
  }

  /**
   * Return the value at the specified index in the key.
   */
  public Object get(int index) {
    if (index < values.size()) {
      return values.get(index);
    }
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    PartitionKey that = (PartitionKey) o;

    if (!values.equals(that.values))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return values != null ? values.hashCode() : 0;
  }

  /**
   * Return the number of values in the key.
   */
  public int getLength() {
    return values.size();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("values", values).toString();
  }

}
