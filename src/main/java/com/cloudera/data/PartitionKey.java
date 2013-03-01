package com.cloudera.data;

import java.util.Arrays;

public class PartitionKey {
  private Object[] values; // TODO: use List

  public PartitionKey(Object[] values) {
    this.values = values;
  }

  public Object[] getValues() {
    return values;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    PartitionKey that = (PartitionKey) o;

    if (!Arrays.equals(values, that.values)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return values != null ? Arrays.hashCode(values) : 0;
  }
}
