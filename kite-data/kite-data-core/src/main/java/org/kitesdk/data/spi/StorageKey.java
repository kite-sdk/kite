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

package org.kitesdk.data.spi;

import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.PartitionStrategy;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * A StorageKey is a complete set of values for a PartitionStrategy.
 *
 * @since 0.9.0
 */
public class StorageKey extends Marker implements Comparable<StorageKey> {

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

  private final PartitionStrategy strategy;
  private final Map<String, Integer> fields;
  private List<Object> values;

  public StorageKey(PartitionStrategy strategy) {
    this(strategy, Arrays.asList(
        new Object[strategy.getFieldPartitioners().size()]));
  }

  private StorageKey(PartitionStrategy strategy, List<Object> values) {
    try {
      this.fields = FIELD_CACHE.get(strategy);
    } catch (ExecutionException ex) {
      throw new RuntimeException("[BUG] Could not get field map");
    }
    Preconditions.checkArgument(values.size() == fields.size(),
        "Not enough values for a complete StorageKey");
    this.strategy = strategy;
    this.values = values;
  }

  public PartitionStrategy getPartitionStrategy() {
    return strategy;
  }

  @Override
  public boolean has(String name) {
    return fields.containsKey(name);
  }

  @Override
  public Object get(String name) {
    return values.get(fields.get(name));
  }

  /**
   * Returns the value for {@code index}.
   *
   * @param index the {@code index} of the value to return
   * @return the Object stored at {@code index}
   */
  public Object get(int index) {
    return values.get(index);
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
   * Replaces all of the values in this {@link StorageKey} with the given List.
   *
   * @param values a List of values
   */
  public void replaceValues(List<Object> values) {
    Preconditions.checkArgument(values != null, "Values cannot be null");
    Preconditions.checkArgument(values.size() == fields.size(),
        "Not enough values for a complete StorageKey");
    this.values = values;
  }

  /**
   * Replaces all of the values in this {@link StorageKey} with values from the given
   * {@code entity}.
   *
   * @param entity an entity to reuse this {@code StorageKey} for
   * @return this updated {@code StorageKey}
   * @throws IllegalStateException
   *      If the {@code entity} cannot be used to produce a value for each
   *      field in the {@code PartitionStrategy}
   *
   * @since 0.9.0
   */
  @SuppressWarnings("unchecked")
  public <E> StorageKey reuseFor(E entity, EntityAccessor<E> accessor) {
    List<FieldPartitioner> partitioners = strategy.getFieldPartitioners();

    for (int i = 0; i < partitioners.size(); i++) {
      FieldPartitioner fp = partitioners.get(i);
      replace(i, fp.apply(accessor.get(entity, fp.getSourceName())));
    }

    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public int compareTo(StorageKey other) {
    if (other == null) {
      throw new NullPointerException("Cannot compare a StorageKey with null");
    } else if (!strategy.equals(other.strategy)) {
      throw new RuntimeException("PartitionStrategy does not match");
    }

    final List<FieldPartitioner> partitioners = strategy.getFieldPartitioners();
    for (int i = 0; i < partitioners.size(); i += 1) {
      final FieldPartitioner fp = partitioners.get(i);
      final int cmp = fp.compare(get(i), other.get(i));
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
    if (obj instanceof StorageKey) {
      return Objects.equal(values, ((StorageKey) obj).values);
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("values", values).toString();
  }

  /**
   * A convenience method to make a copy of a {@link StorageKey}.
   *
   * This is not a deep copy.
   *
   * @param toCopy a {@code StorageKey} to copy
   * @return a new StorageKey with the same {@link PartitionStrategy} and content
   */
  public static StorageKey copy(StorageKey toCopy) {
    return new StorageKey(toCopy.strategy, Lists.newArrayList(toCopy.values));
  }

  /**
   * A fluent {@code Builder} for creating a {@link StorageKey}.
   */
  public static class Builder {
    private final PartitionStrategy strategy;
    private final Map<String, Object> values;

    public Builder(PartitionStrategy strategy) {
      this.strategy = strategy;
      this.values = Maps.newHashMap();
    }

    public Builder(Dataset dataset) {
      if (!dataset.getDescriptor().isPartitioned()) {
        throw new DatasetException("Dataset is not partitioned");
      }
      this.strategy = dataset.getDescriptor().getPartitionStrategy();
      this.values = Maps.newHashMap();
    }

    public Builder add(String name, Object value) {
      this.values.put(name, value);
      return this;
    }

    @SuppressWarnings("unchecked")
    public StorageKey build() {
      final List<FieldPartitioner> partitioners =
          strategy.getFieldPartitioners();
      final List<Object> content = Lists.newArrayListWithCapacity(
          partitioners.size());
      for (FieldPartitioner fp : partitioners) {
        content.add(valueFor(fp));
      }
      return new StorageKey(strategy, content);
    }

    private <S, T> T valueFor(FieldPartitioner<S, T> fp) {
      if (values.containsKey(fp.getName())) {
        return Conversions.convert(values.get(fp.getName()), fp.getType());

      } else if (values.containsKey(fp.getSourceName())) {
        return fp.apply(Conversions.convert(
            values.get(fp.getSourceName()),
            fp.getSourceType()));

      } else {
        throw new IllegalStateException(
            "Cannot create StorageKey, missing data for field:" + fp.getName());
      }
    }
  }
}
