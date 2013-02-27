package com.cloudera.data.partition;

import com.cloudera.data.PartitionStrategy;

/**
 * Convenience class so you can say e.g. <code>hash("username", 2)</code> in JEXL.
 */
public class PartitionFunctions {

  public static PartitionStrategy hash(String name, int buckets) {
    return new HashPartitionStrategy(name, buckets);
  }

  public static PartitionStrategy identity(String name, int buckets) {
    return new IdentityPartitionStrategy(name, buckets);
  }

  public static PartitionStrategy range(String name, int[] upperBounds) {
    return new IntRangePartitionStrategy(name, upperBounds);
  }

}
