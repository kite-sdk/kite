/*
 * Copyright 2013 Cloudera.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.cdk.data.spi;

import com.cloudera.cdk.data.FieldPartitioner;
import com.cloudera.cdk.data.Marker;
import com.cloudera.cdk.data.PartitionStrategy;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

/**
 * A Key is a Marker that is complete for a PartitionStrategy.
 */
public class Key extends Marker implements Comparable<Key> {

  // Cache the field to index mappings for each PartitionStrategy
  private static final LoadingCache<PartitionStrategy, Map<String, Integer>>
      FIELD_CACHE = CacheBuilder.newBuilder().build(
      new CacheLoader<PartitionStrategy, Map<String, Integer>>() {
        @Override
        public Map<String, Integer> load(PartitionStrategy strategy) {
          final List<FieldPartitioner> fields = strategy.getFieldPartitioners();
          final Map<String, Integer> fieldMap = Maps
              .newHashMapWithExpectedSize(fields.size());
          for (int i = 0, n = fields.size(); i < n; i += 1) {
            fieldMap.put(fields.get(i).getName(), i);
          }
          return fieldMap;
        }
      });

  final PartitionStrategy strategy;
  final Map<String, Integer> fields;
  List<Object> values;

  public Key(PartitionStrategy strategy) {
    this(strategy, Arrays.asList(
        new Object[strategy.getFieldPartitioners().size()]));
  }

  public Key(PartitionStrategy strategy, Marker marker) {
    this(strategy);
    reuseFor(marker);
  }

  public Key(PartitionStrategy strategy, List<Object> values) {
    try {
      this.fields = FIELD_CACHE.get(strategy);
    } catch (ExecutionException ex) {
      throw new RuntimeException("[BUG] Could not get field map");
    }
    Preconditions.checkArgument(values.size() == fields.size(),
        "Not enough values for a complete Key");
    this.strategy = strategy;
    this.values = values;
  }

  public Key(PartitionStrategy strategy, Object entity) {
    this(strategy);
    reuseFor(entity);
  }

  public PartitionStrategy getPartitionStrategy() {
    return strategy;
  }

  @Override
  public boolean has(String name) {
    return fields.containsKey(name);
  }

  @Override
  public Object getObject(String name) {
    return values.get(fields.get(name));
  }

  /**
   * Returns the value for {@code index}.
   *
   * @param index the {@code index} of the value to return
   * @return the Object stored at {@code index}
   */
  public Object getObject(int index) {
    return values.get(index);
  }

  /**
   * Returns the value for {@code index} coerced to the given type, T.
   *
   * @param <T> the return type
   * @param index the index of the value to return
   * @param returnType  The return type, which must be assignable from Long,
   *                    Integer, String, or Object
   * @return the Object stored for at {@code index} coerced to a T
   * @throws ClassCastException if the return type is unknown
   */
  public <T> T getAs(int index, Class<T> returnType) {
    return Conversions.convert(getObject(index), returnType);
  }

  /**
   * Replaces the value at {@code index} with the given {@code value}.
   *
   * @param index the {@code index} of the field to replace
   * @param value an Object to store at {@code index}
   */
  public void replace(int index, Object value) {
    values.set(index, value);
  }

  /**
   * Replaces all of the values in this {@link Key} with the given List.
   *
   * @param values a List of values
   */
  public void replaceValues(List<Object> values) {
    Preconditions.checkArgument(values != null, "Values cannot be null");
    Preconditions.checkArgument(values.size() == fields.size(),
        "Not enough values for a complete Key");
    this.values = values;
  }

  /**
   * Replaces all of the values in this {@link Key} with values from the given
   * {@link Marker}.
   *
   * @param marker a {@code Marker} to reuse this {@code Key} for
   * @return this updated {@code Key}
   * @throws IllegalStateException
   *      If the {@code Marker} cannot be used to produce a value for each
   *      field in the {@code PartitionStrategy}
   *
   * @since 0.9.0
   */
  public Key reuseFor(Marker marker) {
    final List<FieldPartitioner> partitioners = strategy.getFieldPartitioners();

    for (int i = 0; i < partitioners.size(); i += 1) {
      final FieldPartitioner fp = partitioners.get(i);
      final Object fieldValue = fp.valueFor(marker);
      if (fieldValue == null) {
        throw new IllegalStateException(
            "Cannot create key, missing data for field:" + fp.getName());
      } else {
        replace(i, fieldValue);
      }
    }

    return this;
  }

  /**
   * Replaces all of the values in this {@link Key} with values from the given
   * {@code entity}.
   *
   * @param entity an entity to reuse this {@code Key} for
   * @return this updated {@code Key}
   * @throws IllegalStateException
   *      If the {@code entity} cannot be used to produce a value for each
   *      field in the {@code PartitionStrategy}
   *
   * @since 0.9.0
   */
  @SuppressWarnings("unchecked")
  public Key reuseFor(Object entity) {
    final List<FieldPartitioner> partitioners = strategy.getFieldPartitioners();

    for (int i = 0; i < partitioners.size(); i++) {
      final FieldPartitioner fp = partitioners.get(i);
      final Object value;
      // TODO: this should probably live elsewhere and be extensible
      if (entity instanceof GenericRecord) {
        value = ((GenericRecord) entity).get(fp.getSourceName());
      } else {
        final String name = fp.getSourceName();
        try {
          PropertyDescriptor propertyDescriptor = new PropertyDescriptor(name,
              entity.getClass(), getter(name), null /* assume read only */);
          value = propertyDescriptor.getReadMethod().invoke(entity);
        } catch (IllegalAccessException e) {
          throw new IllegalStateException("Cannot read property " + name +
              " from " + entity, e);
        } catch (InvocationTargetException e) {
          throw new IllegalStateException("Cannot read property " + name +
              " from " + entity, e);
        } catch (IntrospectionException e) {
          throw new IllegalStateException("Cannot read property " + name +
              " from " + entity, e);
        }
      }
      replace(i, fp.apply(value));
    }

    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public int compareTo(Key other) {
    if (other == null) {
      throw new NullPointerException("Cannot compare a Key with null");
    } else if (!strategy.equals(other.strategy)) {
      throw new RuntimeException("PartitionStrategy does not match");
    }

    final List<FieldPartitioner> partitioners = strategy.getFieldPartitioners();
    for (int i = 0; i < partitioners.size(); i += 1) {
      final FieldPartitioner fp = partitioners.get(i);
      final int cmp = fp.compare(getObject(i), other.getObject(i));
      if (cmp != 0) {
        return cmp;
      }
    }
    return 0;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(values);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Key) {
      return Objects.equal(values, ((Key) obj).values);
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("values", values).toString();
  }

  private static String getter(String name) {
    return "get" +
        name.substring(0, 1).toUpperCase(Locale.ENGLISH) +
        name.substring(1);
  }
}
