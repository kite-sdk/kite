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

package com.cloudera.cdk.data;

import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Map;
import javax.annotation.concurrent.Immutable;

/**
 * A Marker is a bookmark in the partition space, which is the space of each
 * possible {@link PartitionKey} for a {@link PartitionStrategy}.
 *
 * A Marker holds un-ordered values that can be used to create keys by a
 * {@code PartitionStrategy}. It can hold either source values, like a
 * timestamp, or concrete values, like a year.
 *
 * A Marker can be used as a generic placeholder or bookmark for an Entity.
 * For example, when fetching a record from HBase, you can use a Marker to hold
 * the information that {@code PartitionStrategy} uses to build a concrete
 * {@code PartitonKey} and fetch the entity. This is easier than creating a
 * fake entity to fetch the real entity. {@see PartitionStrategy#keyFor(Marker)}
 *
 * A Marker can also be used as a partial key, where some of the values needed
 * to create a complete {@link PartitionKey} are missing. In this case, a
 * Marker contains a subset of the partition space: all of the keys that share
 * the values that are set in the Marker.
 *
 * Markers are Immutable and do not change. To work with Markers, use the
 * {@link Marker.Builder} class, which can copy values from other Markers and
 * easily create new Markers:
 * <pre>
 * // a partial Marker for all events with the same hash code and approx time
 * Marker partial = new Marker.Builder()
 *    .add("shard", id.hashCode())
 *    .add("timestamp", System.getTimeMillis())
 *    .get();
 *
 * // a Marker that represents a particular object
 * Marker concrete = new Marker.Builder(partial).add("id", id).get();
 *
 * // a partial Marker for approx time, for all shards
 * new Marker.Builder("timestamp", System.getTimeMillis()).get();
 * </pre>
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
  public abstract Object getObject(String name);

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
    if (returnType.isAssignableFrom(Long.class)) {
      return returnType.cast(getLong(name));
    } else if (returnType.isAssignableFrom(Integer.class)) {
      return returnType.cast(getInteger(name));
    } else if (returnType.isAssignableFrom(String.class)) {
      return returnType.cast(getString(name));
    } else if (returnType.isAssignableFrom(Object.class)) {
      return returnType.cast(getObject(name));
    } else {
      throw new ClassCastException(
          "[BUG] getAs(String, Class) must be called with " +
          "Long, Integer, String, or Object.");
    }
  }

  /**
   * Returns the value for {@code name} coerced to a Long.
   *
   * @param name the String name of the value to return
   * @return the Object stored for {@code name}
   */
  public Long getLong(String name) {
    return makeLong(getObject(name));
  }

  /**
   * Returns the value for {@code name} coerced to an Integer.
   *
   * @param name the String name of the value to return
   * @return the Object stored for {@code name}
   */
  public Integer getInteger(String name) {
    return makeInteger(getObject(name));
  }

  /**
   * Returns the value for {@code name} coerced to a String.
   *
   * @param name the String name of the value to return
   * @return the Object stored for {@code name}
   */
  public String getString(String name) {
    return makeString(getObject(name));
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

  /**
   * A basic Marker implementation backed by a Map.
   */
  @Immutable
  public static class ImmutableMarker extends Marker {

    final Map<String, Object> values;

    public ImmutableMarker(Map<String, Object> content) {
      this.values = ImmutableMap.copyOf(content);
    }

    @Override
    public boolean has(String name) {
      return values.containsKey(name);
    }

    @Override
    public Object getObject(String name) {
      return values.get(name);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("values", values).toString();
    }

  }

  /**
   * A fluent builder for creating Marker instances.
   *
   * @since 0.9.0
   */
  public static class Builder implements Supplier<Marker> {
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
     * Marker end = new Marker.Builder("timestamp", System.currentTimeMillis()).get();
     * </pre>
     *
     * @param name a String name
     * @param value an Object value
     */
    public Builder(String key, Object value) {
      this.content = Maps.newHashMapWithExpectedSize(1);
      content.put(key, value);
    }

    /**
     * Constructs an empty {@code Builder}.
     */
    public Builder() {
      this.content = Maps.newHashMap();
    }

    /**
     * Clears the content already added to this builder so it can be reused.
     */
    public void clear() {
      content.clear();
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
    @Override
    public Marker get() {
      return new ImmutableMarker(content);
    }
  }
}
