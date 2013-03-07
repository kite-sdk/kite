package com.cloudera.data;

import com.google.common.base.Function;

/**
 * <p>
 * Partitions values for a named field.
 * </p>
 * <p>
 * Used by a {@link PartitionStrategy} to calculate which partition an entity
 * belongs in, based on the value of a given field. A field partitioner can, in
 * some cases, provide meaningful cardinality hints to query systems. A good
 * example of this is a hash partitioner which always knows the number of
 * buckets produced by the function.
 * </p>
 * 
 * @see PartitionStrategy
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

  /**
   * <p>
   * Apply the partition function to the given {@code value}.
   * </p>
   * <p>
   * The type of value must be compatible with the field partitioner
   * implementation. Normally, this is validated at the time of initial
   * configuration rather than at runtime.
   * </p>
   */
  @Override
  public abstract Object apply(Object value);

}
