/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spi.predicates;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.kitesdk.data.spi.SchemaUtil;

public class In<T> extends RegisteredPredicate<T> {
  private static final String IN = "in";
  static {
    RegisteredPredicate.register(IN, new RegisteredPredicate.Factory() {
      @Override
      public <V> RegisteredPredicate<V> fromString(String values, Schema schema) {
        return In.fromString(values, schema);
      }
    });
  }

  public static <T> In<T> fromString(String set, Schema schema) {
    Set<T> values = Sets.newHashSet();
    for (String value : Splitter.on(',').split(set)) {
      values.add(SchemaUtil.<T>fromString(value, schema));
    }
    return Predicates.in(values);
  }

  // ImmutableSet entries are non-null
  private final Set<T> set;

  In(Iterable<T> values) {
    this.set = ImmutableSet.copyOf(values);
    Preconditions.checkArgument(set.size() > 0, "No values to match");
  }

  In(T... values) {
    this.set = ImmutableSet.copyOf(values);
  }

  In(Set<T> set) {
    this.set = set;
  }

  @Override
  public String getName() {
    return IN;
  }

  @Override
  public boolean apply(@Nullable T test) {
    // Set#contains may throw NPE, depending on implementation
    return (test != null) && set.contains(test);
  }

  public In<T> filter(Predicate<? super T> predicate) {
    try {
      return new In<T>(Iterables.filter(set, predicate));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Filter predicate produces empty set", e);
    }
  }

  public <V> In<V> transform(Function<? super T, V> function) {
    return new In<V>(Iterables.transform(set, function));
  }

  Set<T> getSet() {
    return set;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    return Objects.equal(set, ((In) o).set);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(set);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("set", set).toString();
  }

  public String toString(Schema schema) {
    return Joiner.on(',').join(
        Iterables.transform(set, new ToString<T>(schema)));
  }

  private static class ToString<T> implements Function<T, String> {
    private final Schema schema;

    private ToString(Schema schema) {
      this.schema = schema;
    }

    @Override
    public String apply(@Nullable T input) {
      return SchemaUtil.toString(input, schema);
    }
  }
}
