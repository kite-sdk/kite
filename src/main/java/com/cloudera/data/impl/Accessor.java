package com.cloudera.data.impl;

import com.cloudera.data.PartitionKey;
import com.cloudera.data.PartitionStrategy;

/**
 * <p>
 * Class to enforce "friend" access to internal methods in
 * {@link com.cloudera.data} classes that are not a part of the public API.
 * </p>
 * <p>
 * This technique is described in detail in "Practical API Design" by
 * Jaroslav Tulach.
 * </p>
 */
public abstract class Accessor {
  private static volatile Accessor DEFAULT;
  public static Accessor getDefault() {
    Accessor a = DEFAULT;
    if (a != null) {
      return a;
    }
    try {
      Class.forName(PartitionStrategy.class.getName(), true,
          PartitionStrategy.class.getClassLoader());
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    return DEFAULT;
  }

  public static void setDefault(Accessor accessor) {
    if (DEFAULT != null) {
      throw new IllegalStateException();
    }
    DEFAULT = accessor;
  }

  public Accessor() {
  }

  public abstract PartitionKey newPartitionKey(Object... values);

  public abstract PartitionStrategy getSubpartitionStrategy(PartitionStrategy partitionStrategy, int startIndex);
}
