/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.data;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import javax.annotation.concurrent.Immutable;
import org.apache.avro.generic.GenericRecord;

import com.cloudera.data.impl.Accessor;
import com.cloudera.data.partition.HashFieldPartitioner;
import com.cloudera.data.partition.IdentityFieldPartitioner;
import com.cloudera.data.partition.IntRangeFieldPartitioner;
import com.cloudera.data.partition.RangeFieldPartitioner;
import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

/**
 * <p>
 * The strategy used to determine how a dataset is partitioned.
 * </p>
 * <p>
 * A {@code PartitionStrategy} is configured with one or more
 * {@link FieldPartitioner}s upon creation. When a {@link Dataset} is configured
 * with a partition strategy, we say that data is partitioned. Any entities
 * written to a partitioned dataset are evaluated with its
 * {@code PartitionStrategy} which, in turn, produces a {@link PartitionKey}
 * that is used by the dataset implementation to select the proper partition.
 * </p>
 * 
 * @see FieldPartitioner
 * @see PartitionKey
 * @see DatasetDescriptor
 * @see Dataset
 */
@Immutable
public class PartitionStrategy {

  private final List<FieldPartitioner> fieldPartitioners;

  static {
    Accessor.setDefault(new AccessorImpl());
  }

  /**
   * Construct a partition strategy with a variadic array of field partitioners.
   */
  public PartitionStrategy(FieldPartitioner... partitioners) {
    fieldPartitioners = Lists.newArrayList(partitioners);
  }

  /**
   * Construct a partition strategy with a list of field partitioners.
   */
  public PartitionStrategy(List<FieldPartitioner> partitioners) {
    fieldPartitioners = Lists.newArrayList(partitioners);
  }

  /**
   * <p>
   * Get the list of field partitioners used for partitioning.
   * </p>
   * <p>
   * {@link FieldPartitioner}s are returned in the same order they are used
   * during partition selection.
   * </p>
   */
  public List<FieldPartitioner> getFieldPartitioners() {
    return Lists.newArrayList(fieldPartitioners);
  }

  /**
   * <p>
   * Return the cardinality produced by the contained field partitioners.
   * </p>
   * <p>
   * This can be used to aid in calculating resource usage used during certain
   * operations. For example, when writing data to a partitioned dataset, this
   * method can be used to estimate (or discover exactly, depending on the
   * partition functions) how many leaf partitions exist.
   * </p>
   * <p>
   * <strong>Warning:</strong> This method is allowed to lie and should be
   * treated only as a hint. Some partition functions are fixed (e.g. hash
   * modulo number of buckets), while others are open-ended (e.g. discrete
   * value) and depend on the input data.
   * </p>
   * 
   * @return The estimated (or possibly concrete) number of leaf partitions.
   */
  public int getCardinality() {
    int cardinality = 1;
    for (FieldPartitioner fieldPartitioner : fieldPartitioners) {
      cardinality *= fieldPartitioner.getCardinality();
    }
    return cardinality;
  }

  /**
   * <p>
   * Construct a partition key with a variadic array of values corresponding to
   * the field partitioners in this partition strategy.
   * </p>
   * <p>
   * It is permitted to have fewer values than field partitioners, in which case
   * all subpartititions in the unspecified parts of the key are matched by the
   * key.
   * </p>
   * <p>
   * Null values are not permitted.
   * </p>
   */
  public PartitionKey partitionKey(Object... values) {
    return new PartitionKey(values);
  }

  /**
   * <p>
   * Construct a partition key for the given entity.
   * </p>
   * <p>
   * This is a convenient way to find the partition that a given entity would be
   * written to, or to find a partition using objects from the entity domain.
   * </p>
   */
  public PartitionKey partitionKeyForEntity(Object entity) {
    Object[] values = new Object[fieldPartitioners.size()]; // TODO: reuse
    for (int i = 0; i < fieldPartitioners.size(); i++) {
      FieldPartitioner fp = fieldPartitioners.get(i);
      String name = fp.getName();
      Object value;
      if (entity instanceof GenericRecord) {
        value = ((GenericRecord) entity).get(name);
      } else {
        try {
          PropertyDescriptor propertyDescriptor = new PropertyDescriptor(name,
              entity.getClass());
          value = propertyDescriptor.getReadMethod().invoke(entity);
        } catch (IllegalAccessException e) {
          throw new RuntimeException("Cannot read property " + name + " from "
              + entity, e);
        } catch (InvocationTargetException e) {
          throw new RuntimeException("Cannot read property " + name + " from "
              + entity, e);
        } catch (IntrospectionException e) {
          throw new RuntimeException("Cannot read property " + name + " from "
              + entity, e);
        }
      }
      values[i] = fp.apply(value);
    }
    return new PartitionKey(values);
  }

  /**
   * Return a {@link PartitionStrategy} for subpartitions starting at the given
   * index.
   */
  PartitionStrategy getSubpartitionStrategy(int startIndex) {
    if (startIndex == 0) {
      return this;
    }
    if (startIndex >= fieldPartitioners.size()) {
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

  /**
   * A fluent builder to aid in the construction of {@link PartitionStrategy}s.
   */
  public static class Builder implements Supplier<PartitionStrategy> {

    private List<FieldPartitioner> fieldPartitioners = Lists.newArrayList();

    /**
     * Configure a hash partitioner with the specified number of {@code buckets}
     * .
     * 
     * @param name
     *          The entity field name from which to get values to be
     *          partitioned.
     * @param buckets
     *          The number of buckets into which data is to be partitioned.
     * @return An instance of the builder for method chaining.
     */
    public Builder hash(String name, int buckets) {
      fieldPartitioners.add(new HashFieldPartitioner(name, buckets));
      return this;
    }

    /**
     * Configure an identity partitioner with a cardinality hint of
     * {@code buckets} size.
     * 
     * @param name
     *          The entity field name from which to get values to be
     *          partitioned.
     * @param buckets
     *          A hint as to the number of partitions that will be created (i.e.
     *          the number of discrete values for the field {@code name} in the
     *          data).
     * @return An instance of the builder for method chaining.
     * @see IdentityFieldPartitioner
     */
    public Builder identity(String name, int buckets) {
      fieldPartitioners.add(new IdentityFieldPartitioner(name, buckets));
      return this;
    }

    /**
     * Configure a range partitioner with a set of {@code upperBounds}.
     * 
     * @param name
     *          The entity field name from which to get values to be
     *          partitioned.
     * @param upperBounds
     *          A variadic list of upper bounds of each partition.
     * @return An instance of the builder for method chaining.
     * @see IntRangeFieldPartitioner
     */
    public Builder range(String name, int... upperBounds) {
      fieldPartitioners.add(new IntRangeFieldPartitioner(name, upperBounds));
      return this;
    }

    /**
     * Configure a range partitioner with a set of {@code upperBounds}.
     * 
     * @param name
     *          The entity field name from which to get values to be
     *          partitioned.
     * @param upperBounds
     *          A variadic list of upper bounds of each partition.
     * @return An instance of the builder for method chaining.
     * @see RangeFieldPartitioner
     */
    public Builder range(String name, Comparable<?>... upperBounds) {
      fieldPartitioners.add(new RangeFieldPartitioner(name, upperBounds));
      return this;
    }

    /**
     * <p>
     * Get the configured {@link PartitionStrategy} instance.
     * </p>
     * <p>
     * This builder should be considered single use and discarded after a call
     * to this method.
     * </p>
     * 
     * @return The configured instance of {@link PartitionStrategy}.
     */
    @Override
    public PartitionStrategy get() {
      return new PartitionStrategy(fieldPartitioners);
    }

  }

}
