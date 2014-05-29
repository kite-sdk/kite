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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;
import com.google.common.collect.Sets;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.binary.Base64;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.spi.partition.CalendarFieldPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * A set of simultaneous constraints.
 *
 * This class accumulates and manages a set of logical constraints.
 */
@Immutable
public class Constraints implements Serializable{

  private static final long serialVersionUID = -155119355851820161L;

  private static final Logger LOG = LoggerFactory.getLogger(Constraints.class);

  private transient Schema schema;
  private transient Map<String, Predicate> constraints;

  public Constraints(Schema schema) {
    this.schema = schema;
    this.constraints = ImmutableMap.of();
  }

  private Constraints(Schema schema, Map<String, Predicate> constraints,
                      String name, Predicate predicate) {
    this.schema = schema;
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
    return new EntityPredicate<E>(constraints);
  }

  /**
   * Get a {@link Predicate} for testing entity objects that match the given
   * {@link StorageKey}.
   *
   * @param <E> The type of entities to be matched
   * @param key a StorageKey for entities tested with the Predicate
   * @return a Predicate to test if entity objects satisfy this constraint set
   */
  public <E> Predicate<E> toEntityPredicate(StorageKey key) {
    if (key != null) {
      Map<String, Predicate> predicates = minimizeFor(key);
      if (predicates.isEmpty()) {
        return com.google.common.base.Predicates.alwaysTrue();
      }
      return new EntityPredicate<E>(predicates);
    }
    return toEntityPredicate();
  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  Map<String, Predicate> minimizeFor(StorageKey key) {
    Map<String, Predicate> unsatisfied = Maps.newHashMap(constraints);
    PartitionStrategy strategy = key.getPartitionStrategy();
    Set<String> timeFields = Sets.newHashSet();
    int i = 0;
    for (FieldPartitioner fp : strategy.getFieldPartitioners()) {
      String field = fp.getSourceName();
      if (fp instanceof CalendarFieldPartitioner) {
        // keep track of time fields to consider
        timeFields.add(field);
      }
      // remove the field if it is satisfied by the StorageKey
      Predicate original = unsatisfied.get(field);
      if (original != null) {
        Predicate isSatisfiedBy = fp.projectStrict(original);
        LOG.debug("original: " + original + ", strict: " + isSatisfiedBy);
        if ((isSatisfiedBy != null) && isSatisfiedBy.apply(key.get(i))) {
          LOG.debug("removing " + field + " satisfied by " + key.get(i));
          unsatisfied.remove(field);
        }
      }
      i += 1;
    }
    // remove fields satisfied by the time predicates
    for (String timeField : timeFields) {
      Predicate<Long> original = unsatisfied.get(timeField);
      if (original != null) {
        Predicate<Marker> isSatisfiedBy = TimeDomain.get(strategy, timeField)
            .projectStrict(original);
        LOG.debug("original: " + original + ", strict: " + isSatisfiedBy);
        if ((isSatisfiedBy != null) && isSatisfiedBy.apply(key)) {
          LOG.debug("removing " + timeField + " satisfied by " + key);
          unsatisfied.remove(timeField);
        }
      }
    }
    return ImmutableMap.copyOf(unsatisfied);
  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  Map<String, Predicate> minimizeFor(
      PartitionStrategy strategy, MarkerRange keyRange) {
    Map<String, Predicate> unsatisfied = Maps.newHashMap(constraints);
    Set<String> timeFields = Sets.newHashSet();
    for (FieldPartitioner fp : strategy.getFieldPartitioners()) {
      String field = fp.getSourceName();
      if (fp instanceof CalendarFieldPartitioner) {
        // keep track of time fields to consider
        timeFields.add(field);
      }
      String partitionName = fp.getName();
      // add the non-time field if it is not satisfied by the MarkerRange
      Predicate original = unsatisfied.get(field);
      if (original != null) {
        Predicate isSatisfiedBy = fp.projectStrict(original);
        Marker start = keyRange.getStart().getBound();
        Marker end = keyRange.getEnd().getBound();
        // check both endpoints. this duplicates a lot of work because we are
        // using Markers rather than the original predicates
        if ((isSatisfiedBy != null) &&
            !isSatisfiedBy.apply(start.get(partitionName)) &&
            !isSatisfiedBy.apply(end.get(partitionName))) {
          unsatisfied.remove(field);
        }
      }
    }
    // add any time predicates that aren't satisfied by the MarkerRange
    for (String timeField : timeFields) {
      Predicate<Long> original = unsatisfied.get(timeField);
      if (original != null) {
        Predicate<Marker> isSatisfiedBy = TimeDomain.get(strategy, timeField)
            .projectStrict(original);
        // check both endpoints. this duplicates a lot of work because we are
        // using Markers rather than the original predicates
        if ((isSatisfiedBy != null) &&
            isSatisfiedBy.apply(keyRange.getStart().getBound()) &&
            isSatisfiedBy.apply(keyRange.getEnd().getBound())) {
          unsatisfied.remove(timeField);
        }
      }
    }
    return ImmutableMap.copyOf(unsatisfied);
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

  /**
   * If this returns true, the entities selected by this set of constraints
   * align to partition boundaries.
   *
   * For example, for a partition strategy [hash(num), identity(num)],
   * any constraint on the "num" field will be correctly enforced by the
   * partition predicate for this constraint set. However, a "color" field
   * wouldn't be satisfied by considering partition values alone and would
   * require further checks.
   *
   * An alternate explanation: This returns whether the key {@link Predicate}
   * from {@link #toKeyPredicate()} is equivalent to this set of constraints
   * under the given {@link PartitionStrategy}. The key predicate must accept a
   * key if that key's partition might include entities matched by this
   * constraint set. If this method returns true, then all entities in the
   * partitions it matches are guaranteed to match this constraint set. So, the
   * partitions are equivalent to the constraints.
   *
   * @param strategy a {@link PartitionStrategy}
   * @return true if this constraint set is satisfied by partitioning
   */
  @SuppressWarnings("unchecked")
  public boolean alignedWithBoundaries(PartitionStrategy strategy) {
    Multimap<String, FieldPartitioner> partitioners = HashMultimap.create();
    for (FieldPartitioner fp : strategy.getFieldPartitioners()) {
      partitioners.put(fp.getSourceName(), fp);
    }

    // The key predicate is equivalent to a constraint set when the permissive
    // projection for each predicate can be used in its place. This happens if
    // fp.project(predicate) == fp.projectStrict(predicate):
    //
    // let D = some value domain
    // let pred : D -> {0, 1}
    // let D_{pred} = {x \in D | pred(x) == 1} (a subset of D selected by pred)
    //
    // let fp : D -> S (also a value domain)
    // let fp.strict(pred) = pred_{fp.strict} : S -> {0, 1}    (project strict)
    //      s.t. pred_{fp.strict}(fp(x)) == 1 => pred(x) == 1
    // let fp.project(pred) = pred_{fp.project} : S -> {0, 1}         (project)
    //      s.t. pred(x) == 1 => pred_{fp.project}(fp(x)) == 1
    //
    // lemma. {x \in D | pred_{fp.strict}(fp(x))} is a subset of D_{pred}
    //     pred_{fp.strict}(fp(x)) == 1 => pred(x) == 1 => x \in D_{pred}
    //
    // theorem. (pred_{fp.project}(s) => pred_{fp.strict}(s)) =>
    //                D_{pred} == {x \in D | pred_{fp.strict}(fp(x))}
    //
    //  => let x \in D_{pred}. then pred_{fp.project}(fp(x)) == 1 by def
    //                         then pred_{fp.strict(fp(x)) == 1 by premise
    //     therefore {x \in D | pred_{fp.strict}(fp(x))} \subsetOf D_{pred}
    //  <= by previous lemma
    //
    // Note: if projectStrict is too conservative or project is too permissive,
    // then this logic cannot determine that that the original predicate is
    // satisfied
    for (Map.Entry<String, Predicate> entry : constraints.entrySet()) {
      Collection<FieldPartitioner> fps = partitioners.get(entry.getKey());
      if (fps.isEmpty()) {
        LOG.debug("No field partitioners for key {}", entry.getKey());
        return false;
      }

      Predicate predicate = entry.getValue();
      if (!(predicate instanceof Predicates.Exists)) {
        boolean satisfied = false;
        for (FieldPartitioner fp : fps) {
          if (fp instanceof CalendarFieldPartitioner) {
            TimeDomain domain = TimeDomain.get(strategy, entry.getKey());
            Predicate strict = domain.projectStrict(predicate);
            Predicate permissive = domain.project(predicate);
            LOG.debug("Time predicate strict: {}", strict);
            LOG.debug("Time predicate permissive: {}", permissive);
            satisfied = strict != null && strict.equals(permissive);
            break;
          } else {
            Predicate strict = fp.projectStrict(predicate);
            Predicate permissive = fp.project(predicate);
            if (strict != null && strict.equals(permissive)) {
              satisfied = true;
              break;
            }
          }
        }
        // this predicate cannot be satisfied by the partition information
        if (!satisfied) {
          LOG.debug("Predicate not satisfied: {}", predicate);
          return false;
        }
      }
    }

    return true;
  }

  @SuppressWarnings("unchecked")
  public Constraints with(String name, Object... values) {
    SchemaUtil.checkTypeConsistency(schema, name, values);
    if (values.length > 0) {
      checkContained(name, values);
      // this is the most specific constraint and is idempotent under "and"
      return new Constraints(schema, constraints, name,
          new Predicates.In<Object>(values));
    } else {
      if (!constraints.containsKey(name)) {
        // no other constraint => add the exists
        return new Constraints(schema, constraints, name, Predicates.exists());
      } else {
        // satisfied by an existing constraint
        return this;
      }
    }
  }

  public Constraints from(String name, Comparable value) {
    SchemaUtil.checkTypeConsistency(schema, name, value);
    checkContained(name, value);
    Range added = Ranges.atLeast(value);
    if (constraints.containsKey(name)) {
      return new Constraints(schema, constraints, name,
          and(constraints.get(name), added));
    } else {
      return new Constraints(schema, constraints, name, added);
    }
  }

  public Constraints fromAfter(String name, Comparable value) {
    SchemaUtil.checkTypeConsistency(schema, name, value);
    checkContained(name, value);
    Range added = Ranges.greaterThan(value);
    if (constraints.containsKey(name)) {
      return new Constraints(schema, constraints, name,
          and(constraints.get(name), added));
    } else {
      return new Constraints(schema, constraints, name, added);
    }
  }

  public Constraints to(String name, Comparable value) {
    SchemaUtil.checkTypeConsistency(schema, name, value);
    checkContained(name, value);
    Range added = Ranges.atMost(value);
    if (constraints.containsKey(name)) {
      return new Constraints(schema, constraints, name,
          and(constraints.get(name), added));
    } else {
      return new Constraints(schema, constraints, name, added);
    }
  }

  public Constraints toBefore(String name, Comparable value) {
    SchemaUtil.checkTypeConsistency(schema, name, value);
    checkContained(name, value);
    Range added = Ranges.lessThan(value);
    if (constraints.containsKey(name)) {
      return new Constraints(schema, constraints, name,
          and(constraints.get(name), added));
    } else {
      return new Constraints(schema, constraints, name, added);
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
  @VisibleForTesting
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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Constraints that = (Constraints) o;
    return Objects.equal(this.constraints, that.constraints) &&
        Objects.equal(this.schema, that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(schema, constraints);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).addValue(constraints).toString();
  }

  /**
   * Writes out the {@link Constraints} using Java serialization.
   */
  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
    out.writeUTF(schema.toString());
    ConstraintsSerialization.writeConstraints(schema, constraints, out);
  }


  /**
   * Reads in the {@link Constraints} from the provided {@code in} stream.
   * @param in the stream from which to deserialize the object.
   * @throws IOException error deserializing the {@link Constraints}
   * @throws ClassNotFoundException Unable to properly access values inside the {@link Constraints}
  */
  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException{
    in.defaultReadObject();
    schema = new Parser().parse(in.readUTF());
    constraints = ImmutableMap.copyOf(ConstraintsSerialization.readConstraints(schema, in));
  }

  public static String serialize(Constraints constraints) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream out = new ObjectOutputStream(baos);
      out.writeObject(constraints);
      out.close();
      return Base64.encodeBase64String(baos.toByteArray());
    } catch (IOException e) {
      throw new DatasetIOException("Cannot serialize constraints " + constraints, e);
    }
  }

  public static Constraints deserialize(String s) {
    try {
      ByteArrayInputStream bais = new ByteArrayInputStream(Base64.decodeBase64(s));
      ObjectInputStream in = new ObjectInputStream(bais);
      return (Constraints) in.readObject();
    } catch (IOException e) {
      throw new DatasetIOException("Cannot deserialize constraints", e);
    } catch (ClassNotFoundException e) {
      throw new DatasetException("Cannot deserialize constraints", e);
    }
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
        Predicate<Marker> timePredicate = TimeDomain
            .get(strategy, sourceName)
            .project(predicates.get(sourceName));
        LOG.debug("Time predicate: {}", timePredicate);
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
