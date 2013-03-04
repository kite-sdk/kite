package com.cloudera.data.impl;

import com.google.common.base.Objects;

import java.util.Arrays;
import java.util.List;

public class PartitionKey {

  private List<Object> values;

  public PartitionKey(Object... values) {
    this.values = Arrays.asList(values);
  }

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

  public int getLength() {
    return values.size();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("values", values).toString();
  }

}
