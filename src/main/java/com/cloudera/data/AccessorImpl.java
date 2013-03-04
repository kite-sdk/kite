package com.cloudera.data;

import com.cloudera.data.impl.Accessor;
import com.cloudera.data.impl.PartitionKey;

final class AccessorImpl extends Accessor {

  @Override
  public PartitionKey getPartitionKey(PartitionStrategy partitionStrategy, Object entity) {
    return partitionStrategy.getPartitionKey(entity);
  }

  @Override
  public PartitionStrategy getSubpartitionStrategy(PartitionStrategy partitionStrategy, int startIndex) {
    return partitionStrategy.getSubpartitionStrategy(startIndex);
  }
}
