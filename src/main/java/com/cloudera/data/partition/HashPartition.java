package com.cloudera.data.partition;

public class HashPartition extends Partition {

  public HashPartition(String name, int buckets) {
    this(name, buckets, null);
  }

  public HashPartition(String name, int buckets, Partition subpartition) {
    super(name, buckets, subpartition);
  }

  @Override
  public Object evaluate(Object value) throws Exception {
    return (value.hashCode() & Integer.MAX_VALUE) % getCardinality();
  }
}
