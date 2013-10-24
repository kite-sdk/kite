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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * A Key is a Marker that is complete for a PartitionStrategy.
 */
public class Key extends Marker {

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

  final Map<String, Integer> fields;
  List<Object> values;

  public Key(PartitionStrategy strategy) {
    this(strategy, Arrays.asList(
        new Object[strategy.getFieldPartitioners().size()]));
  }

  public Key(PartitionStrategy strategy, List<Object> values) {
    try {
      this.fields = FIELD_CACHE.get(strategy);
    } catch (ExecutionException ex) {
      throw new RuntimeException("[BUG] Could not get field map");
    }
    Preconditions.checkArgument(values.size() == fields.size(),
        "Not enough values for a complete Key");
    this.values = values;
  }

  // for copying other Keys
  Key(Map<String, Integer> fields, List<Object> values) {
    Preconditions.checkArgument(values.size() == fields.size(),
        "Not enough values for a complete Key");
    this.fields = fields;
    this.values = values;
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
   * @param name the String name of the value to return
   * @param returnType  The return type, which must be assignable from Long,
   *                    Integer, String, or Object
   * @return the Object stored for at {@code index} coerced to a T
   * @throws ClassCastException if the return type is unknown
   */
  public <T> T getAs(int index, Class<T> returnType) {
    if (returnType.isAssignableFrom(Long.class)) {
      return returnType.cast(getLong(index));
    } else if (returnType.isAssignableFrom(Integer.class)) {
      return returnType.cast(getInteger(index));
    } else if (returnType.isAssignableFrom(String.class)) {
      return returnType.cast(getString(index));
    } else if (returnType.isAssignableFrom(Object.class)) {
      return returnType.cast(getObject(index));
    } else {
      throw new ClassCastException(
          "[BUG] getAs(int, Class) must be called with " +
          "Long, Integer, String, or Object.");
    }
  }

  /**
   * Returns the value for {@code index} coerced to a Long.
   *
   * @param index the index of the value to return
   * @return the Object stored for {@code index}
   */
  public Long getLong(int index) {
    return makeLong(getObject(index));
  }

  /**
   * Returns the value for {@code index} coerced to an Integer.
   *
   * @param index the index of the value to return
   * @return the Object stored for {@code index}
   */
  public Integer getInteger(int index) {
    return makeInteger(getObject(index));
  }

  /**
   * Returns the value for {@code index} coerced to a String.
   *
   * @param index the index of the value to return
   * @return the Object stored for {@code index}
   */
  public String getString(int index) {
    return makeString(getObject(index));
  }

  /**
   * Replaces the value at index {@code index} with the given {@code value}.
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
    Preconditions.checkArgument(values.size() == fields.size(),
        "Not enough values for a complete Key");
    this.values = values;
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

  static Long makeLong(Object value) {
    if (value instanceof Number) {
      return ((Number) value).longValue();
    } else if (value instanceof String) {
      return Long.valueOf((String) value);
    } else {
      throw new RuntimeException(
          "Cannot coerce \"" + value + "\" to Long");
    }
  }

  static Integer makeInteger(Object value) {
    if (value instanceof Number) {
      return ((Number) value).intValue();
    } else if (value instanceof String) {
      return Integer.valueOf((String) value);
    } else {
      throw new RuntimeException(
          "Cannot coerce \"" + value + "\" to Integer");
    }
  }

  static String makeString(Object value) {
    // start simple, but we may want more complicated conversion here
    return value.toString();
  }

}

