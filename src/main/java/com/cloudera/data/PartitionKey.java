package com.cloudera.data;

import java.util.Arrays;
import java.util.List;

public class PartitionKey {

  private List<Object> values;

  public PartitionKey(Object... values) {
    this.values = Arrays.asList(values);
  }

  public List<Object> getValues() {
    return values;
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

}
