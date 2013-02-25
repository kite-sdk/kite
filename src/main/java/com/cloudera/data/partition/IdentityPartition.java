package com.cloudera.data.partition;

public class IdentityPartition extends Partition {

  public IdentityPartition(String name, int buckets) {
    this(name, buckets, null);
  }

  public IdentityPartition(String name, int buckets, Partition subpartition) {
    super(name, buckets, subpartition);
  }

  @Override
  public Object evaluate(Object value) throws Exception {
    return value;
  }
}
