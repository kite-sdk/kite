package com.cloudera.data;

import com.google.common.base.Function;

/**
 * <p>
 * Partitions values for a named field.
 * </p>
 * <p>
 * Used by a {@link PartitionStrategy} to calculate which partition an entity belongs in.
 * </p>
 */
public abstract class FieldPartitioner implements Function<Object, Object> {
  private String name;
  private int cardinality;

  protected FieldPartitioner(String name, int cardinality) {
    this.name = name;
    this.cardinality = cardinality;
  }

  public String getName() {
    return name;
  }

  public int getCardinality() {
    return cardinality;
  }
}
