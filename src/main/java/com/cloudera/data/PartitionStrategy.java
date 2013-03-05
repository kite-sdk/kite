package com.cloudera.data;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.apache.avro.generic.GenericRecord;

import com.cloudera.data.partition.HashFieldPartitioner;
import com.cloudera.data.partition.IdentityFieldPartitioner;
import com.cloudera.data.partition.IntRangeFieldPartitioner;
import com.cloudera.data.partition.RangeFieldPartitioner;
import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

public class PartitionStrategy {

  private List<FieldPartitioner> fieldPartitioners;

  protected PartitionStrategy() {
    fieldPartitioners = Lists.newArrayList();
  }

  public PartitionStrategy(FieldPartitioner... partitioners) {
    this();

    for (FieldPartitioner fieldPartitioner : partitioners) {
      addFieldPartitioner(fieldPartitioner);
    }
  }

  public PartitionStrategy(List<FieldPartitioner> partitioners) {
    this();

    fieldPartitioners.addAll(partitioners);
  }

  private void addFieldPartitioner(FieldPartitioner fieldPartitioner) {
    fieldPartitioners.add(fieldPartitioner);
  }

  public List<FieldPartitioner> getFieldPartitioners() {
    return fieldPartitioners;
  }

  // TODO: replace with name of first field partitioner?
  public String getName() {
    StringBuilder sb = new StringBuilder();
    for (FieldPartitioner fieldPartitioner : fieldPartitioners) {
      if (sb.length() > 0) {
        sb.append("/");
      }
      sb.append(fieldPartitioner.getName());
    }
    return sb.toString();
  }

  public int getCardinality() {
    int cardinality = 1;
    for (FieldPartitioner fieldPartitioner : fieldPartitioners) {
      cardinality *= fieldPartitioner.getCardinality();
    }
    return cardinality;
  }

  /**
   * Returns a key that represents the value of the partition.
   */
  public PartitionKey getPartitionKey(Object entity) {
    Object[] values = new Object[fieldPartitioners.size()]; // TODO: reuse
    try {
      for (int i = 0; i < fieldPartitioners.size(); i++) {
        FieldPartitioner fp = fieldPartitioners.get(i);
        if (entity instanceof GenericRecord) {
          values[i] = fp.apply(((GenericRecord) entity).get(fp.getName()));
        } else {
          PropertyDescriptor propertyDescriptor = new PropertyDescriptor(
              fp.getName(), entity.getClass());
          Object value = propertyDescriptor.getReadMethod().invoke(entity);
          values[i] = fp.apply(value);
        }
      }
      return new PartitionKey(values);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Cannot read property " + getName() + " from "
          + entity, e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException("Cannot read property " + getName() + " from "
          + entity, e);
    } catch (IntrospectionException e) {
      throw new RuntimeException("Cannot read property " + getName() + " from "
          + entity, e);
    }
  }

  /**
   * Return a {@link PartitionStrategy} for subpartitions starting at the given
   * index.
   */
  public PartitionStrategy getSubpartitionStrategy(int startIndex) {
    if (startIndex == 0) {
      return this;
    }
    if (startIndex == fieldPartitioners.size()) {
      return null;
    }
    return new PartitionStrategy(fieldPartitioners.subList(startIndex,
        fieldPartitioners.size()));
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("fieldPartitioners", fieldPartitioners).toString();
  }

  public static class Builder implements Supplier<PartitionStrategy> {

    private PartitionStrategy partitionStrategy = new PartitionStrategy();

    public Builder hash(String name, int buckets) {
      partitionStrategy.addFieldPartitioner(new HashFieldPartitioner(name,
          buckets));
      return this;
    }

    public Builder identity(String name, int buckets) {
      partitionStrategy.addFieldPartitioner(new IdentityFieldPartitioner(name,
          buckets));
      return this;
    }

    public Builder range(String name, int... upperBounds) {
      partitionStrategy.addFieldPartitioner(new IntRangeFieldPartitioner(name,
          upperBounds));
      return this;
    }

    public Builder range(String name, Comparable<?>... upperBounds) {
      partitionStrategy.addFieldPartitioner(new RangeFieldPartitioner(name,
          upperBounds));
      return this;
    }

    @Override
    public PartitionStrategy get() {
      return partitionStrategy;
    }

  }

}
