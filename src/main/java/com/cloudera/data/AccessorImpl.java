package com.cloudera.data;

import com.cloudera.data.impl.Accessor;

final class AccessorImpl extends Accessor {

  @Override
  public PartitionKey newPartitionKey(Object... values) {
    return new PartitionKey(values);
  }

  @Override
  public PartitionStrategy getSubpartitionStrategy(PartitionStrategy partitionStrategy, int startIndex) {
    return partitionStrategy.getSubpartitionStrategy(startIndex);
  }
}
