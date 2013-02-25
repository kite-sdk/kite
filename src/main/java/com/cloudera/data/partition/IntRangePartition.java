package com.cloudera.data.partition;

public class IntRangePartition extends Partition {

  int[] upperBounds;

  public IntRangePartition(String name, int... upperBounds) {
    this(null, name, upperBounds.length);
  }

  public IntRangePartition(Partition subpartition, String name, int... upperBounds) {
    super(name, upperBounds.length, subpartition);
    this.upperBounds = upperBounds;
  }

  @Override
  public Object evaluate(Object value) throws Exception {
    Integer val = (Integer) value;
    for (int i = 0; i < upperBounds.length; i++) {
      if (val <= upperBounds[i]) {
        return i;
      }
    }
    throw new IllegalArgumentException(value + " is outside bounds");
  }
}
