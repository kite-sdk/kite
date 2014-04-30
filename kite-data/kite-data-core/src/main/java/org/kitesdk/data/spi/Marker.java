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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 * A {@code Marker} is a bookmark in the partition space, which is the space of
 * each possible partition in a {@link org.kitesdk.data.PartitionStrategy}.
 *
 * A {@code Marker} holds un-ordered values that can be used to create keys by
 * a {@code PartitionStrategy}. It can hold either source values, like a
 * timestamp, or concrete values, like a year.
 *
 * A {@code Marker} can be used as a generic placeholder or bookmark for an
 * entity (an object in a {@link org.kitesdk.data.Dataset}. For example, when fetching a record
 * from HBase, you can use a {@code Marker} to hold the information that
 * {@code PartitionStrategy} uses to build a concrete key and fetch the entity.
 *
 * A {@code Marker} can also be used as a partial key, where some of the values
 * needed to create a complete key are missing. In this case, a {@code Marker}
 * contains a subset of the partition space: all {@code Marker} the keys that
 * share the values that are set in the Marker.
 *
 * The {@code Marker.Builder} class is the easiest way to make a {@code Marker}.
 * Any {@code Marker} created by the {@link Marker.Builder} is {@link Immutable}
 * and will not change. The {@code Builder} can copy values from other
 * {@code Marker} objects and provides a fluent interface to easily create new
 * instances:
 * <pre>
 * // a partial Marker for all events with the same hash code and approx time
 * Marker partial = new Marker.Builder()
 *    .add("shard", id.hashCode())
 *    .add("timestamp", System.getTimeMillis())
 *    .build();
 *
 * // a Marker that represents a particular object
 * Marker concrete = new Marker.Builder(partial).add("id", id).build();
 *
 * // a partial Marker for approx time, for all shards
 * new Marker.Builder("timestamp", System.getTimeMillis()).build();
 * </pre>
 *
 * @since 0.9.0
 */
public abstract class Marker {

  /**
   * Returns whether {@code name} is stored in this Marker.
   *
   * @param name a String name
   * @return true if there is a value for {@code name}
   */
  public abstract boolean has(String name);

  /**
   * Returns the value for {@code name}.
   *
   * @param name the String name of the value to return
   * @return the Object stored for {@code name}
   */
  public abstract Object get(String name);

  /**
   * Returns the value for {@code name} coerced to the given type, T.
   *
   * @param <T> the return type
   * @param name the String name of the value to return
   * @param returnType  The return type, which must be assignable from Long,
   *                    Integer, String, or Object
   * @return the Object stored for {@code name} coerced to a T
   * @throws ClassCastException if the return type is unknown
   */
  public <T> T getAs(String name, Class<T> returnType) {
    return Conversions.convert(get(name), returnType);
  }

  /**
   * Return the value of a {@code FieldPartitioner} field for this {@link Marker}.
   *
   * If the {@code Marker} has a value for the field's name, that value is
   * returned using {@link Marker#getAs(java.lang.String, java.lang.Class)}. If
   * the {@code Marker} only has a value for the the source field name, then
   * that value is retrieved using
   * {@link org.kitesdk.data.spi.Marker#getAs(java.lang.String,
   * java.lang.Class)} and the field's transformation is applied to it as the source value.
   *
   * @param fp a {@code FieldPartitioner}
   * @return the value of the field for this {@code marker}, or null
   * @since 0.9.0
   */
  @Nullable
  public <S, T> T valueFor(FieldPartitioner<S, T> fp) {
    if (has(fp.getName())) {
      return getAs(fp.getName(), fp.getType());
    } else if (has(fp.getSourceName())) {
      return fp.apply(getAs(fp.getSourceName(), fp.getSourceType()));
    } else {
      return null;
    }
  }

  /**
   * A basic Marker implementation backed by a Map.
   *
   * @since 0.9.0
   */
  @Immutable
  static class ImmutableMarker extends Marker {

    final Map<String, Object> values;

    ImmutableMarker(Map<String, Object> content) {
      this.values = ImmutableMap.copyOf(content);
    }

    @Override
    public boolean has(String name) {
      return values.containsKey(name);
    }

    @Override
    public Object get(String name) {
      return values.get(name);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("values", values).toString();
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(values);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || !getClass().equals(obj.getClass())) {
        return false;
      }
      return Objects.equal(this.values, ((ImmutableMarker) obj).values);
    }
  }

  /**
   * A fluent builder for creating Marker instances.
   *
   * @since 0.9.0
   */
  public static class Builder {
    private final Map<String, Object> content;

    /**
     * Constructs a {@code Builder} from the contents of a {@code Marker}.
     *
     * @param toCopy a Marker that will be copied
     */
    public Builder(ImmutableMarker toCopy) {
      this(toCopy.values);
    }

    /**
     * Constructs a {@code Builder} from the contents of another {@code Builder}.
     *
     * @param toCopy a Builder that will be copied
     */
    public Builder(Builder toCopy) {
      this(toCopy.content);
    }

    /**
     * Constructs a {@code Builder} from the contents of a {@code Map}.
     *
     * @param content a Map from String names to value Objects
     */
    public Builder(Map<String, Object> content) {
      // defensive copy: don't trust the incoming map isn't Immutable or reused
      this.content = Maps.newHashMap(content);
    }

    /**
     * A convenience constructor for a {@code Builder} with only one value.
     *
     * <pre>
     * // this makes sense when using only Calendar partition fields
     * Marker end = new Marker.Builder("timestamp", System.currentTimeMillis()).build();
     * </pre>
     *
     * @param name a String name
     * @param value an Object value
     */
    public Builder(String name, Object value) {
      this.content = Maps.newHashMapWithExpectedSize(1);
      content.put(name, value);
    }

    /**
     * Constructs an empty {@code Builder}.
     */
    public Builder() {
      this.content = Maps.newHashMap();
    }

    /**
     * Adds a named value to this {@code Builder}.
     *
     * @param name a String name for the value
     * @param value a value
     * @return this Builder, for method chaining
     */
    public Builder add(String name, Object value) {
      content.put(name, value);
      return this;
    }

    /**
     * Builds a {@link Marker} from the content added to this {@code Builder}.
     *
     * @return a Marker for the content in this Builder.
     */
    public Marker build() {
      return new ImmutableMarker(content);
    }
  }
}
