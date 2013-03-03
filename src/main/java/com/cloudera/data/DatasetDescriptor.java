package com.cloudera.data;

import org.apache.avro.Schema;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;

public class DatasetDescriptor {

  private Schema schema;
  private PartitionStrategy partitionStrategy;

  public DatasetDescriptor(Schema schema, PartitionStrategy partitionStrategy) {
    this.schema = schema;
    this.partitionStrategy = partitionStrategy;
  }

  public Schema getSchema() {
    return schema;
  }

  public PartitionStrategy getPartitionStrategy() {
    return partitionStrategy;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("schema", schema)
        .add("partitionStrategy", partitionStrategy).toString();
  }

  public static class Builder implements Supplier<DatasetDescriptor> {

    private Schema schema;
    private PartitionStrategy partitionStrategy;

    public Builder schema(Schema schema) {
      this.schema = schema;
      return this;
    }

    public Builder partitionStrategy(PartitionStrategy partitionStrategy) {
      this.partitionStrategy = partitionStrategy;
      return this;
    }

    @Override
    public DatasetDescriptor get() {
      Preconditions.checkState(schema != null,
          "Descriptor schema may not be null");

      return new DatasetDescriptor(schema, partitionStrategy);
    }

  }

}
