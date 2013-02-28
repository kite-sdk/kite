package com.cloudera.data.partition;

import com.cloudera.data.FieldPartitioner;
import com.cloudera.data.PartitionStrategy;

public class IdentityFieldPartitioner extends FieldPartitioner {

  public IdentityFieldPartitioner(String name, int buckets) {
    super(name, buckets);
  }

  @Override
  public Object apply(Object value) {
    return value;
  }

}
