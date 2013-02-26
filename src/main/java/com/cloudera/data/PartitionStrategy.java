package com.cloudera.data;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import org.apache.avro.generic.GenericRecord;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;

public abstract class PartitionStrategy implements Function<Object, Object> {

  private String name;
  private int cardinality;
  private PartitionStrategy partition;

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
        * (isPartitioned() ? getPartition().getCardinality() : 1);
  }

  public PartitionStrategy getPartition() {
    return partition;
  }

  public boolean isPartitioned() {
    return partition != null;
  }

  public Object applyToEntity(Object entity) {
    // TODO: this should be formalized, and it should be possible to have a different property name to the partition name
    try {
      if (entity instanceof GenericRecord) {
        return ((GenericRecord) entity).get(getName());
      }
      PropertyDescriptor propertyDescriptor = new PropertyDescriptor(getName(), entity.getClass());
      Object value = propertyDescriptor.getReadMethod().invoke(entity);
      return apply(value);
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

}
