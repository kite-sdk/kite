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
package org.kitesdk.data;

import com.google.common.base.Objects;

import java.util.Arrays;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * <p>
 * A key for retrieving partitions from a {@link Dataset}.
 * </p>
 * <p>
 * A {@code PartitionKey} is a ordered sequence of values corresponding to the
 * {@link org.kitesdk.data.spi.FieldPartitioner}s in a {@link PartitionStrategy}. A
 * {@link PartitionKey} may be obtained using
 * {@link PartitionStrategy#partitionKey(Object...)} or
 * {@link PartitionStrategy#partitionKeyForEntity(Object)}.
 * </p>
 * <p>
 * Implementations of {@link PartitionKey} are typically not thread-safe; that is,
 * the behavior when accessing a single instance from multiple threads is undefined.
 * </p>
 * 
 * @see PartitionStrategy
 * @see org.kitesdk.data.spi.FieldPartitioner
 * @see Dataset
 */
@NotThreadSafe
public class PartitionKey {

  private final Object[] values;

  PartitionKey(Object... values) {
    this.values = values;
  }

  PartitionKey(int size) {
    this.values = new Object[size];
  }

  public List<Object> getValues() {
    return Arrays.asList(values);
  }

  /**
   * Return the value at the specified index in the key.
   */
  public Object get(int index) {
    if (index < values.length) {
      return values[index];
    }
    return null;
  }

  void set(int index, Object value) {
    values[index] = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || !getClass().equals(o.getClass())) {
      return false;
    }

    PartitionKey that = (PartitionKey) o;

    return Arrays.equals(values, that.values);
  }

  @Override
  public int hashCode() {
    return values != null ? Arrays.hashCode(values) : 0;
  }

  /**
   * Return the number of values in the key.
   */
  public int getLength() {
    return values.length;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("values", getValues()).toString();
  }

}
