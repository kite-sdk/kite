package com.cloudera.data.partition;

import com.cloudera.data.PartitionStrategy;

public class IdentityPartitionStrategy extends PartitionStrategy {

  public IdentityPartitionStrategy(String name, int buckets) {
    this(name, buckets, null);
  }

  public IdentityPartitionStrategy(String name, int buckets, PartitionStrategy subpartition) {
    super(name, buckets, subpartition);
  }

  @Override
  public Object apply(Object value) {
    return value;
  }

}
