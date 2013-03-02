package com.cloudera.data.partition;

import com.cloudera.data.FieldPartitioner;

public class HashFieldPartitioner extends FieldPartitioner {

  public HashFieldPartitioner(String name, int buckets) {
    super(name, buckets);
  }

  @Override
  public Object apply(Object value) {
    return (value.hashCode() & Integer.MAX_VALUE) % getCardinality();
  }

}
