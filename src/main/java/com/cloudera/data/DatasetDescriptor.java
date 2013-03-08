package com.cloudera.data;

import javax.annotation.Nullable;

import org.apache.avro.Schema;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;

/**
 * <p>
 * The structural definition of a {@link Dataset}.
 * </p>
 * <p>
 * Each {@code Dataset} has an associated {@link Schema} and optional
 * {@link PartitionStrategy} defined at the time of creation. Instances of this
 * class are used to hold this information. Users are strongly encouraged to use
 * the inner {@link Builder} to create new instances.
 * </p>
 */
public class DatasetDescriptor {

  private Schema schema;
  private PartitionStrategy partitionStrategy;

  /**
   * Create an instance of this class with the supplied {@link Schema} and
   * optional {@link PartitionStrategy}.
   */
  public DatasetDescriptor(Schema schema,
      @Nullable PartitionStrategy partitionStrategy) {

    this.schema = schema;
    this.partitionStrategy = partitionStrategy;
  }

  public Schema getSchema() {
    return schema;
  }

  @Nullable
  public PartitionStrategy getPartitionStrategy() {
    return partitionStrategy;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("schema", schema)
        .add("partitionStrategy", partitionStrategy).toString();
  }

  /**
   * A fluent builder to aid in the construction of {@link DatasetDescriptor}s.
   */
  public static class Builder implements Supplier<DatasetDescriptor> {

    private Schema schema;
    private PartitionStrategy partitionStrategy;

    /**
     * Configure the dataset's schema. Required.
     * 
     * @return An instance of the builder for method chaining.
     */
    public Builder schema(Schema schema) {
      this.schema = schema;
      return this;
    }

    /**
     * Configure the dataset's partitioning strategy. Optional.
     * 
     * @return An instance of the builder for method chaining.
     */
    public Builder partitionStrategy(@Nullable PartitionStrategy partitionStrategy) {
      this.partitionStrategy = partitionStrategy;
      return this;
    }

    /**
     * Get an instance of the configured dataset descriptor. Subsequent calls
     * will produce new instances that are similarly configure.
     */
    @Override
    public DatasetDescriptor get() {
      Preconditions.checkState(schema != null,
          "Descriptor schema may not be null");

      return new DatasetDescriptor(schema, partitionStrategy);
    }

  }

}
