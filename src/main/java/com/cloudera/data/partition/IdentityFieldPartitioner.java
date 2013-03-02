package com.cloudera.data.partition;

import com.cloudera.data.FieldPartitioner;

public class IdentityFieldPartitioner extends FieldPartitioner {

  public IdentityFieldPartitioner(String name, int buckets) {
    super(name, buckets);
  }

  @Override
  public Object apply(Object value) {
    return value;
  }

}
