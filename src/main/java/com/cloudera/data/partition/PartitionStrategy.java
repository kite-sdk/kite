package com.cloudera.data.partition;

import com.google.common.base.Function;
import com.google.common.base.Objects;

public abstract class PartitionStrategy implements Function<Object, Object> {

  private String name;
  private int cardinality;
  private PartitionStrategy partition;

  protected PartitionStrategy(String name, int cardinality,
      PartitionStrategy partition) {

    this.name = name;
    this.cardinality = cardinality;
    this.partition = partition;
  }

  public String getName() {
    return name;
  }

  public int getCardinality() {
    return cardinality;
  }

  public int getAggregateCardinality() {
    return cardinality
        * (isPartitioned() ? getPartition().getCardinality() : 1);
  }

  public PartitionStrategy getPartition() {
    return partition;
  }

  public boolean isPartitioned() {
    return partition != null;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", name)
        .add("cardinality", cardinality).add("partition", partition).toString();
  }

}
