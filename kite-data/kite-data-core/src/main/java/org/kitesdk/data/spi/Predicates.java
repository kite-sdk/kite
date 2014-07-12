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

package org.kitesdk.data.spi;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.kitesdk.data.DatasetException;

public abstract class Predicates {
  @SuppressWarnings("unchecked")
  public static <T> Exists<T> exists() {
    return (Exists<T>) Exists.INSTANCE;
  }

  @SuppressWarnings("unchecked")
  public static <T> In<T> in(Set<T> set) {
    T item = Iterables.getFirst(set, null);
    if (item != null && item instanceof CharSequence) {
      return (In<T>) new In(new CharSequences.ImmutableCharSequenceSet(
          (Set<CharSequence>) set));
    }
    return new In<T>(set);
  }

  @SuppressWarnings("unchecked")
  public static <T> In<T> in(T... set) {
    T item = set[0];
    if (item != null && item instanceof CharSequence) {
      return (In<T>) new In(new CharSequences.ImmutableCharSequenceSet(
          (Set<CharSequence>) Sets.newHashSet(set)));
    }
    return new In<T>(set);
  }

  public static <T> String toString(Predicate<T> predicate, Schema schema) {
    if (predicate instanceof RegisteredPredicate) {
      return ((RegisteredPredicate) predicate).toString(schema);
    } else if (predicate instanceof Range) {
      return ((Range) predicate).toString(schema);
    } else if (predicate instanceof In) {
      return ((In) predicate).toString(schema);
    } else {
      throw new DatasetException("Unknown predicate: " + predicate);
    }
  }

  public static <T> Predicate<T> fromString(String pString, Schema schema) {
    Predicate<T> predicate = Range.fromString(pString, schema);
    if (predicate != null) {
      return predicate;
    }
    predicate = RegisteredPredicate.fromString(pString, schema);
    if (predicate != null) {
      return predicate;
    }
    predicate = In.fromString(pString, schema);
    if (predicate != null) {
      return predicate;
    }
    throw new DatasetException("Unknown predicate: " + pString);
  }

  // This should be a method on Range, like In#transform.
  // Unfortunately, Range is final so we will probably need to re-implement it.
  public static <S extends Comparable, T extends Comparable>
  Range<T> transformClosed(Range<S> range, Function<? super S, T> function) {
    if (range.hasLowerBound()) {
      if (range.hasUpperBound()) {
        return Ranges.closed(
            function.apply(range.lowerEndpoint()),
            function.apply(range.upperEndpoint()));
      } else {
        return Ranges.atLeast(function.apply(range.lowerEndpoint()));
      }
    } else if (range.hasUpperBound()) {
      return Ranges.atMost(function.apply(range.upperEndpoint()));
    } else {
      return null;
    }
  }

  public static <T extends Comparable>
  Range<T> adjustClosed(Range<T> range, DiscreteDomain<T> domain) {
    // adjust to a closed range to avoid catching extra keys
    if (range.hasLowerBound()) {
      T lower = range.lowerEndpoint();
      if (range.isLowerBoundOpen()) {
        lower = domain.next(lower);
      }
      if (range.hasUpperBound()) {
        T upper = range.upperEndpoint();
        if (range.isUpperBoundOpen()) {
          upper = domain.previous(upper);
        }
        return Ranges.closed(lower, upper);
      } else {
        return Ranges.atLeast(lower);
      }
    } else if (range.hasUpperBound()) {
      T upper = range.upperEndpoint();
      if (range.isUpperBoundOpen()) {
        upper = domain.previous(upper);
      }
      return Ranges.atMost(upper);
    } else {
      throw new IllegalArgumentException("Invalid range: no endpoints");
    }
  }

  static {
    RegisteredPredicate.register("exists", new RegisteredPredicate.Factory() {
      @Override
      public <T> RegisteredPredicate<T> fromString(String empty, Schema _) {
        return exists();
      }
    });
  }

  public static class Exists<T> extends RegisteredPredicate<T> {
    public static final Exists INSTANCE = new Exists();

    private Exists() {
    }

    @Override
    public boolean apply(@Nullable T value) {
      return (value != null);
    }

    @Override
    public String getName() {
      return "exists";
    }

    @Override
    public String toString(Schema _) {
      return "";
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).toString();
    }
  }

  public static class In<T> implements Predicate<T> {
    public static <T> In<T> fromString(String set, Schema schema) {
      Set<T> values = Sets.newHashSet();
      for (String value : Splitter.on(',').split(set)) {
        values.add(SchemaUtil.<T>fromString(value, schema));
      }
      return Predicates.in(values);
    }

    // ImmutableSet entries are non-null
    private final Set<T> set;

    private In(Iterable<T> values) {
      this.set = ImmutableSet.copyOf(values);
      Preconditions.checkArgument(set.size() > 0, "No values to match");
    }

    private In(T... values) {
      this.set = ImmutableSet.copyOf(values);
    }

    private In(Set<T> set) {
      this.set = set;
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
}
