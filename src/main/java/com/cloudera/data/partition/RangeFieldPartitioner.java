package com.cloudera.data.partition;

import java.util.Arrays;
import java.util.List;

import com.cloudera.data.FieldPartitioner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class RangeFieldPartitioner extends FieldPartitioner {

  private List<Comparable<?>> upperBounds;

  public RangeFieldPartitioner(String name,
      Comparable<?>... upperBounds) {

    super(name, upperBounds.length);
    this.upperBounds = Arrays.asList(upperBounds);
  }

  @Override
  public Object apply(Object value) {
    Preconditions.checkArgument(value instanceof Comparable<?>,
        "Unable to range partition a value that isn't comparable:%s", value);

    @SuppressWarnings("unchecked")
    Comparable<? super Object> val = (Comparable<? super Object>) value;

    for (Comparable<?> comparable : upperBounds) {
      if (val.compareTo(comparable) <= 0) {
        return comparable;
      }
    }

    throw new IllegalArgumentException(value + " is outside bounds");
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("upperBounds", upperBounds)
        .toString();
  }
}
