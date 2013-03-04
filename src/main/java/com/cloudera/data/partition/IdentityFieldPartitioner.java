package com.cloudera.data.partition;

import com.cloudera.data.FieldPartitioner;
import com.google.common.base.Objects;

public class IdentityFieldPartitioner extends FieldPartitioner {

  public IdentityFieldPartitioner(String name, int buckets) {
    super(name, buckets);
  }

  @Override
  public Object apply(Object value) {
    return value;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", getName())
        .add("cardinality", getCardinality()).toString();
  }
}
