package com.cloudera.data.partition;

import java.util.List;
import java.util.Set;

import com.cloudera.data.FieldPartitioner;

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

}
