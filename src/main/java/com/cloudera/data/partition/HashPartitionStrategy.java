package com.cloudera.data.partition;

import com.cloudera.data.PartitionStrategy;

public class HashPartitionStrategy extends PartitionStrategy {

  public HashPartitionStrategy(String name, int buckets) {
    this(name, buckets, null);
  }

  public HashPartitionStrategy(String name, int buckets,
      PartitionStrategy subpartition) {

    super(name, buckets, subpartition);
  }

  @Override
  public Object apply(Object value) {
    return (value.hashCode() & Integer.MAX_VALUE) % getCardinality();
  }

}
