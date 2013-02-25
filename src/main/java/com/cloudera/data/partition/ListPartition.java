package com.cloudera.data.partition;

import java.util.List;
import java.util.Set;

public class ListPartition extends Partition {

  List<Set<?>> values;

  public ListPartition(String name, List<Set<?>> values) {
    this(name, values, null);
  }

  public ListPartition(String name, List<Set<?>> values, Partition subpartition) {
    super(name, cardinality(values), subpartition);
    this.values = values;
  }

  private static int cardinality(List<Set<?>> values) {
    int c = 0;
    for(Set<?> set : values) {
      c += set.size();
    }
    return c;
  }

  @Override
  protected Object evaluate(Object value) throws Exception {
    for (int i = 0; i < values.size(); i++) {
      if (values.get(i).contains(value)) {
        return i;
      }
    }
    throw new IllegalArgumentException(value + " is not in set");
  }
}
