package com.cloudera.data.partition;

import javax.annotation.Nonnull;

import com.cloudera.data.FieldPartitioner;
import com.google.common.base.Objects;

public class HashFieldPartitioner extends FieldPartitioner {

  public HashFieldPartitioner(String name, int buckets) {
    super(name, buckets);
  }

  @Override
  public Object apply(@Nonnull Object value) {
    return (value.hashCode() & Integer.MAX_VALUE) % getCardinality();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", getName())
        .add("cardinality", getCardinality()).toString();
  }
}
