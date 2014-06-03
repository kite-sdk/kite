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
package org.kitesdk.data;

import org.apache.avro.Schema;
import org.kitesdk.data.spi.Conversions;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.SchemaUtil;

/**
 * <p>
 * A key for retrieving entities from a {@link RandomAccessDataset}.
 * </p>
 *
 * @since 0.9.0
 */
public class Key {

  private final List<Object> values;

  Key(List<Object> values) {
    this.values = values;
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

  /**
   * A fluent builder to aid in the construction of {@link Key} objects.
   *
   * @since 0.9.0
   */
  public static class Builder {

    private Schema schema;
    private PartitionStrategy strategy;
    private Set<String> fieldNames;
    private final Map<String, Object> values;

    /**
     * Construct a {@link Builder} for a {@link RandomAccessDataset}.
     */
    public Builder(RandomAccessDataset dataset) {
      this.schema = dataset.getDescriptor().getSchema();
      this.strategy = dataset.getDescriptor().getPartitionStrategy();
      this.fieldNames = Sets.newHashSet();
      for (FieldPartitioner fp : strategy.getFieldPartitioners()) {
        fieldNames.add(fp.getSourceName());
        fieldNames.add(fp.getName());
      }
      this.values = Maps.newHashMap();
    }

    /**
     * Add a key value for the named field.
     *
     * @throws IllegalArgumentException If the there is no key field named
     * <code>name</code> for this builder's dataset.
     * @return An instance of the builder for method chaining.
     */
    public Builder add(String name, Object value) {
      Preconditions.checkArgument(fieldNames.contains(name), "Field %s not in schema.",
          name);
      values.put(name, value);
      return this;
    }

    /**
     * Build an instance of the configured key.
     *
     * @throws IllegalStateException If any required key field is missing.
     */
    @SuppressWarnings("unchecked")
    public Key build() {
      final List<FieldPartitioner> partitioners = strategy.getFieldPartitioners();
      final List<Object> content = Lists.newArrayListWithCapacity(partitioners.size());
      for (FieldPartitioner fp : partitioners) {
        content.add(valueFor(fp));
      }
      return new Key(content);
    }

    @SuppressWarnings("unchecked")
    private <S, T> T valueFor(FieldPartitioner<S, T> fp) {
      if (values.containsKey(fp.getName())) {
        return Conversions.convert(values.get(fp.getName()),
            SchemaUtil.getPartitionType(fp, schema));

      } else if (values.containsKey(fp.getSourceName())) {
        return fp.apply(Conversions.convert(values.get(fp.getSourceName()),
            SchemaUtil.getSourceType(fp, schema)));

      } else {
        throw new IllegalStateException(
            "Cannot create Key, missing data for field:" + fp.getName());
      }
    }
  }
}
