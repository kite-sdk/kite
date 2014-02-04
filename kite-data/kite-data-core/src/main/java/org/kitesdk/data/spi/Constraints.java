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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;
import com.google.common.collect.Sets;
import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.apache.avro.generic.GenericRecord;
import org.kitesdk.data.FieldPartitioner;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.partition.CalendarFieldPartitioner;

/**
 * A set of simultaneous constraints.
 *
 * This class accumulates and manages a set of logical constraints.
 */
@Immutable
public class Constraints {

  private final Map<String, Predicate> constraints;

  public Constraints() {
    this.constraints = ImmutableMap.of();
  }

  private Constraints(Map<String, Predicate> constraints,
                      String name, Predicate predicate) {
    Map<String, Predicate> copy = Maps.newHashMap(constraints);
    copy.put(name, predicate);
    this.constraints = ImmutableMap.copyOf(copy);
  }

  /**
   * Get a {@link Predicate} for testing entity objects.
   *
   * @param <E> The type of entities to be matched
   * @return a Predicate to test if entity objects satisfy this constraint set
   */
  public <E> Predicate<E> toEntityPredicate() {
    // TODO: Filter constraints that are definitely satisfied by a StorageKey
    return new EntityPredicate<E>(constraints);
  }

  /**
   * Get a {@link Predicate} that tests {@link StorageKey} objects.
   *
   * If a {@code StorageKey} matches the predicate, it <em>may</em> represent a
   * partition that is responsible for entities that match this set of
   * constraints. If it does not match the predicate, it cannot be responsible
   * for entities that match this constraint set.
   *
   * @return a Predicate for testing StorageKey objects
   */
  public Predicate<StorageKey> toKeyPredicate() {
    return new KeyPredicate(constraints);
  }

  /**
   * Get a set of {@link MarkerRange} objects that covers the set of possible
   * {@link StorageKey} partitions for this constraint set, with respect to the
   * give {@link PartitionStrategy}. If a {@code StorageKey} is not in one of
   * the ranges returned by this method, then its partition cannot contain
   * entities that satisfy this constraint set.
   *
   * @param strategy a PartitionStrategy
   * @return an Iterable of MarkerRange
   */
  public Iterable<MarkerRange> toKeyRanges(PartitionStrategy strategy) {
    return new KeyRangeIterable(strategy, constraints);
  }

  @SuppressWarnings("unchecked")
  public Constraints with(String name, Object... values) {
    if (values.length > 0) {
      checkContained(name, values);
      // this is the most specific constraint and is idempotent under "and"
      return new Constraints(constraints, name, new Predicates.In<Object>(values));
    } else {
      if (!constraints.containsKey(name)) {
        // no other constraint => add the exists
        return new Constraints(constraints, name, Predicates.exists());
      } else {
        // satisfied by an existing constraint
        return this;
      }
    }
  }

  public Constraints from(String name, Comparable value) {
    checkContained(name, value);
    Range added = Ranges.atLeast(value);
    if (constraints.containsKey(name)) {
      return new Constraints(constraints, name,
          and(constraints.get(name), added));
    } else {
      return new Constraints(constraints, name, added);
    }
  }

  public Constraints fromAfter(String name, Comparable value) {
    checkContained(name, value);
    Range added = Ranges.greaterThan(value);
    if (constraints.containsKey(name)) {
      return new Constraints(constraints, name,
          and(constraints.get(name), added));
    } else {
      return new Constraints(constraints, name, added);
    }
  }

  public Constraints to(String name, Comparable value) {
    checkContained(name, value);
    Range added = Ranges.atMost(value);
    if (constraints.containsKey(name)) {
      return new Constraints(constraints, name,
          and(constraints.get(name), added));
    } else {
      return new Constraints(constraints, name, added);
    }
  }

  public Constraints toBefore(String name, Comparable value) {
    checkContained(name, value);
    Range added = Ranges.lessThan(value);
    if (constraints.containsKey(name)) {
      return new Constraints(constraints, name,
          and(constraints.get(name), added));
    } else {
      return new Constraints(constraints, name, added);
    }
  }

  /**
   * Returns the predicate for a named field.
   *
   * For testing.
   *
   * @param name a String field name
   * @return a Predicate for the given field, or null if none is set
   */
  Predicate get(String name) {
    return constraints.get(name);
  }

  @SuppressWarnings("unchecked")
  private void checkContained(String name, Object... values) {
    for (Object value : values) {
      if (constraints.containsKey(name)) {
        Predicate current = constraints.get(name);
        Preconditions.checkArgument(current.apply(value),
            "%s does not match %s", current, value);
      }
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).addValue(constraints).toString();
  }

  @SuppressWarnings("unchecked")
  private static Predicate and(Predicate previous, Range additional) {
    if (previous instanceof Range) {
      // return the intersection
      return ((Range) previous).intersection(additional);
    } else if (previous instanceof Predicates.In) {
      // filter the set using the range
      return ((Predicates.In) previous).filter(additional);
    } else if (previous instanceof Predicates.Exists) {
      // exists is the weakest constraint, satisfied by any existing constraint
      // all values in the range are non-null
      return additional;
    } else {
      // previous must be null, return the new constraint
      return additional;
    }
  }

  /**
   * A {@link Predicate} for testing entities against a set of predicates.
   *
   * @param <E> The type of entities this predicate tests
   */
  private static class EntityPredicate<E> implements Predicate<E> {
    private final Map<String, Predicate> predicates;

    public EntityPredicate(Map<String, Predicate> predicates) {
      this.predicates = predicates;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean apply(@Nullable E entity) {
      if (entity == null) {
        return false;
      }
      // check each constraint and fail immediately
      for (Map.Entry<String, Predicate> entry : predicates.entrySet()) {
        if (!entry.getValue().apply(get(entity, entry.getKey()))) {
          return false;
        }
      }
      // all constraints were satisfied
      return true;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || !getClass().equals(obj.getClass())) {
        return false;
      }
      return Objects.equal(predicates, ((EntityPredicate) obj).predicates);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(predicates);
    }

    private static Object get(Object entity, String name) {
      if (entity instanceof GenericRecord) {
        return ((GenericRecord) entity).get(name);
      } else {
        try {
          PropertyDescriptor propertyDescriptor = new PropertyDescriptor(name,
              entity.getClass(), getter(name), null /* assume read only */);
          return propertyDescriptor.getReadMethod().invoke(entity);
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
    }

    private static String getter(String name) {
      return "get" +
          name.substring(0, 1).toUpperCase(Locale.ENGLISH) +
          name.substring(1);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).addValue(predicates).toString();
    }
  }

  /**
   * A {@link Predicate} for testing a {@link StorageKey} against a set of
   * predicates.
   */
  private static class KeyPredicate implements Predicate<StorageKey> {
    private final Map<String, Predicate> predicates;

    private KeyPredicate(Map<String, Predicate> predicates) {
      this.predicates = predicates;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean apply(StorageKey key) {
      if (key == null) {
        return false;
      }
      PartitionStrategy strategy = key.getPartitionStrategy();
      // (source) time fields that affect the partition strategy
      Set<String> timeFields = Sets.newHashSet();

      // this is fail-fast: if the key fails a constraint, then drop it
      for (FieldPartitioner fp : strategy.getFieldPartitioners()) {
        Predicate constraint = predicates.get(fp.getSourceName());
        if (constraint == null) {
          // no constraints => anything matches
          continue;
        }

        Object pValue = key.get(fp.getName());

        if (fp instanceof CalendarFieldPartitioner) {
          timeFields.add(fp.getSourceName());
        }

        Predicate projectedConstraint = fp.project(constraint);
        if (projectedConstraint != null && !(projectedConstraint.apply(pValue))) {
          return false;
        }
      }

      // check multi-field time groups
      for (String sourceName : timeFields) {
        Predicate<StorageKey> timePredicate = TimeDomain
            .get(strategy, sourceName)
            .project(predicates.get(sourceName));
        if (timePredicate != null && !timePredicate.apply(key)) {
          return false;
        }
      }

      // if we made it this far, everything passed
      return true;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).addValue(predicates).toString();
    }
  }
}
