package com.cloudera.data.partition;

import com.cloudera.data.FieldPartitioner;

/**
 * Convenience class so you can say e.g. <code>hash("username", 2)</code> in
 * JEXL.
 */
public class PartitionFunctions {

  public static FieldPartitioner hash(String name, int buckets) {
    return new HashFieldPartitioner(name, buckets);
  }

  public static FieldPartitioner identity(String name, int buckets) {
    return new IdentityFieldPartitioner(name, buckets);
  }

  public static FieldPartitioner range(String name, int[] upperBounds) {
    return new IntRangeFieldPartitioner(name, upperBounds);
  }

  public static FieldPartitioner range(String name, Comparable<?> upperBounds) {
    return new RangeFieldPartitioner(name, upperBounds);
  }

}
