package com.cloudera.data;

import com.google.common.base.Objects;

import java.util.Arrays;
import java.util.List;

/**
 * <p>
 * A key for retrieving partitions from a {@link Dataset}.
 * </p>
 * <p>
 * A {@code PartitionKey} is a ordered sequence of values corresponding to the
 * {@link FieldPartitioner}s in a {@link PartitionStrategy}.
 * A {@link PartitionKey} may be obtained using
 * {@link PartitionStrategy#partitionKey(Object...)} or
 * {@link PartitionStrategy#partitionKeyForEntity(Object)}.
 * </p>
 *
 * @see PartitionStrategy
 * @see FieldPartitioner
 * @see Dataset
 */
public class PartitionKey {

  private List<Object> values;

  PartitionKey(Object... values) {
    this.values = Arrays.asList(values);
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
