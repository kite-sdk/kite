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
package org.kitesdk.data.spi;

import com.google.common.base.Objects;

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import org.kitesdk.data.PartitionStrategy;

/**
 * <p>
 * A key for retrieving partitions from a {@link org.kitesdk.data.Dataset}.
 * </p>
 * <p>
 * A {@code PartitionKey} is an ordered sequence of values corresponding to the
 * {@link org.kitesdk.data.spi.FieldPartitioner}s in a
 * {@link org.kitesdk.data.PartitionStrategy}. You can obtain a {@link PartitionKey} using
 * {@link PartitionKey#partitionKeyForEntity(org.kitesdk.data.PartitionStrategy, Object, EntityAccessor, PartitionKey)}.
 * </p>
 * <p>
 * Implementations of {@link PartitionKey} are typically not thread-safe; that 
 * is, the behavior when accessing a single instance from multiple threads is
 * undefined.
 * </p>
 * 
 * @see org.kitesdk.data.PartitionStrategy
 * @see org.kitesdk.data.spi.FieldPartitioner
 * @see org.kitesdk.data.Dataset
 */
@NotThreadSafe
public class PartitionKey {

  private final Object[] values;

  public PartitionKey(Object... values) {
    this.values = values;
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

  protected void set(int index, Object value) {
    values[index] = value;
  }

  /**
   * <p>
   * Construct a partition key for the given entity.
   * </p>
   * <p>
   * This is a convenient way to find the partition that a given entity is
   * written to, or to find a partition using objects from the entity domain.
   * </p>
   */
  public static <E> PartitionKey partitionKeyForEntity(PartitionStrategy strategy,
      E entity, EntityAccessor<E> accessor) {
    return partitionKeyForEntity(strategy, entity, accessor, null);
  }

  /**
   * <p>
   * Construct a partition key for the given entity, reusing the supplied key if
   * not null.
   * </p>
   * <p>
   * This is a convenient way to find the partition that a given entity is
   * written to, or to find a partition using objects from the entity domain.
   * </p>
   */
  @SuppressWarnings("unchecked")
  public static <E> PartitionKey partitionKeyForEntity(PartitionStrategy strategy,
      E entity, EntityAccessor<E> accessor, @Nullable PartitionKey reuseKey) {
    List<FieldPartitioner> fieldPartitioners = strategy.getFieldPartitioners();

    PartitionKey key = (reuseKey == null ?
        new PartitionKey(new Object[fieldPartitioners.size()]) : reuseKey);

    for (int i = 0; i < fieldPartitioners.size(); i++) {
      FieldPartitioner fp = fieldPartitioners.get(i);
      key.set(i, fp.apply(accessor.get(entity, fp.getSourceName())));
    }
    return key;
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
