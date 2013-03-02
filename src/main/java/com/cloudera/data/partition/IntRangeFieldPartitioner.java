package com.cloudera.data.partition;

import com.cloudera.data.FieldPartitioner;

public class IntRangeFieldPartitioner extends FieldPartitioner {

  private int[] upperBounds;

  public IntRangeFieldPartitioner(String name, int... upperBounds) {
    super(name, upperBounds.length);
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
