package com.cloudera.data;

import com.cloudera.data.partition.HashPartitionStrategy;
import com.cloudera.data.partition.IdentityPartitionStrategy;
import com.cloudera.data.partition.IntRangePartitionStrategy;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import org.apache.avro.generic.GenericRecord;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

public abstract class PartitionStrategy implements Function<Object, Object> {

  private String name;
  private int cardinality;
  private PartitionStrategy partition;

  protected PartitionStrategy(String name, int cardinality) {
    this(name, cardinality, null);
  }

  protected PartitionStrategy(String name, int cardinality,
      PartitionStrategy partition) {

    this.name = name;
    this.cardinality = cardinality;
    this.partition = partition;
  }

  public String getName() {
    return name;
  }

  public int getCardinality() {
    return cardinality;
  }

  public int getAggregateCardinality() {
    return cardinality
        * (isPartitioned() ? getPartitionStrategy().getCardinality() : 1);
  }

  public PartitionStrategy getPartitionStrategy() {
    return partition;
  }

  void setPartitionStrategy(PartitionStrategy partitionStrategy) {
    this.partition = partitionStrategy;
  }

  public boolean isPartitioned() {
    return partition != null;
  }

  public String getLabels(Object entity) {
    // TODO: this should be formalized, and it should be possible to have a different property name to the partition name
    try {
      String label;
      if (entity instanceof GenericRecord) {
        label = apply(((GenericRecord) entity).get(getName())).toString();
      } else {
        PropertyDescriptor propertyDescriptor = new PropertyDescriptor(getName(), entity.getClass());
        Object value = propertyDescriptor.getReadMethod().invoke(entity);
        label = apply(value).toString();
      }
      return isPartitioned() ? label + "/" + getPartitionStrategy().getLabels(entity) : label;
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Cannot read property " + getName() + " from " + entity, e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException("Cannot read property " + getName() + " from " + entity, e);
    } catch (IntrospectionException e) {
      throw new RuntimeException("Cannot read property " + getName() + " from " + entity, e);
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", name)
        .add("cardinality", cardinality).add("partition", partition).toString();
  }

  public static class Builder implements Supplier<PartitionStrategy> {

    private PartitionStrategy partitionStrategy;

    private void addPartitionStrategy(PartitionStrategy strategy) {
      if (partitionStrategy == null) {
        partitionStrategy = strategy;
      } else {
        partitionStrategy.setPartitionStrategy(strategy);
      }
    }

    public Builder hash(String name, int buckets) {
      addPartitionStrategy(new HashPartitionStrategy(name, buckets));
      return this;
    }

    public Builder identity(String name, int buckets) {
      addPartitionStrategy(new IdentityPartitionStrategy(name, buckets));
      return this;
    }

    public Builder range(String name, int... upperBounds) {
      addPartitionStrategy(new IntRangePartitionStrategy(name, upperBounds));
      return this;
    }

    @Override
    public PartitionStrategy get() {
      return partitionStrategy;
    }
  }

}
