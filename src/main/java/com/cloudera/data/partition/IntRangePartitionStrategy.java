package com.cloudera.data.partition;

public class IntRangePartitionStrategy extends PartitionStrategy {

  private int[] upperBounds;

  public IntRangePartitionStrategy(String name, int... upperBounds) {
    this(null, name, upperBounds.length);
  }

  public IntRangePartitionStrategy(PartitionStrategy subpartition, String name,
      int... upperBounds) {
    super(name, upperBounds.length, subpartition);
    this.upperBounds = upperBounds;
  }

  @Override
  public Object apply(Object value) {
    Integer val = (Integer) value;

    for (int i = 0; i < upperBounds.length; i++) {
      if (val <= upperBounds[i]) {
        return i;
      }
    }

    throw new IllegalArgumentException(value + " is outside bounds");
  }

}
