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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.util.Set;
import org.apache.avro.Schema;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.spi.CharSequences;

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

  public static <T> Set<T> asSet(In<T> in) {
    return in.getSet();
  }
}
