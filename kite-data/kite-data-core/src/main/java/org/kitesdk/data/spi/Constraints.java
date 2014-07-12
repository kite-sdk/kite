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
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.reflect.ReflectData;
import org.apache.commons.codec.binary.Base64;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.spi.partition.CalendarFieldPartitioner;
import org.kitesdk.data.spi.predicates.Exists;
import org.kitesdk.data.spi.predicates.In;
import org.kitesdk.data.spi.predicates.Predicates;
import org.kitesdk.data.spi.predicates.Range;
import org.kitesdk.data.spi.predicates.Ranges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Predicates.alwaysTrue;

/**
 * A set of simultaneous constraints.
 *
 * This class accumulates combine manages a set of logical constraints.
 */
@Immutable
public class Constraints implements Serializable{

  private static final long serialVersionUID = -155119355851820161L;

  private static final Logger LOG = LoggerFactory.getLogger(Constraints.class);

  private transient Schema schema;
  private transient PartitionStrategy strategy;
  private transient Map<String, Predicate> constraints;

  public Constraints(Schema schema) {
    this(schema, null);
  }

  public Constraints(Schema schema, PartitionStrategy strategy) {
    this.schema = schema;
    this.strategy = strategy;
    this.constraints = ImmutableMap.of();
  }

  private Constraints(Schema schema, PartitionStrategy strategy,
                      Map<String, Predicate> constraints) {
    this.schema = schema;
    this.strategy = strategy;
    this.constraints = constraints;
  }

  private Constraints(Schema schema, PartitionStrategy strategy,
                      Map<String, Predicate> constraints,
                      String name, Predicate predicate) {
    this.schema = schema;
    this.strategy = strategy;
    Map<String, Predicate> copy = Maps.newHashMap(constraints);
    copy.put(name, predicate);
    this.constraints = ImmutableMap.copyOf(copy);
  }

  @VisibleForTesting
  Constraints partitionedBy(PartitionStrategy strategy) {
    return new Constraints(schema, strategy, constraints);
  }

  /**
   * Get a {@link Predicate} for testing entity objects.
   *
   * @param <E> The type of entities to be matched
   * @return a Predicate to test if entity objects satisfy this constraint set
   */
  public <E> Predicate<E> toEntityPredicate() {
    return entityPredicate(constraints, schema, strategy);
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
        return alwaysTrue();
      }
      return entityPredicate(predicates, schema, strategy);
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
      String partition = fp.getName();
      Predicate partitionPredicate = unsatisfied.get(partition);
      if (partitionPredicate != null && partitionPredicate.apply(key.get(i))) {
        unsatisfied.remove(partition);
        LOG.debug("removing " + partition + " satisfied by " + key.get(i));
      }

      String source = fp.getSourceName();
      if (fp instanceof CalendarFieldPartitioner) {
        // keep track of time fields to consider
        timeFields.add(source);
      }
      // remove the field if it is satisfied by the StorageKey
      Predicate original = unsatisfied.get(source);
      if (original != null) {
        Predicate isSatisfiedBy = fp.projectStrict(original);
        LOG.debug("original: " + original + ", strict: " + isSatisfiedBy);
        if ((isSatisfiedBy != null) && isSatisfiedBy.apply(key.get(i))) {
          LOG.debug("removing " + source + " satisfied by " + key.get(i));
          unsatisfied.remove(source);
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

  /**
   * Get a {@link Predicate} that tests {@link StorageKey} objects.
   *
   * If a {@code StorageKey} matches the predicate, it <em>may</em> represent a
   * partition that is responsible for entities that match this set of
   * constraints. If it does not match the predicate, it cannot be responsible
   * for entities that match this constraint set.
   *
   * @return a Predicate for testing StorageKey objects
   * @throws NullPointerException if no partition strategy is defined
   */
  public Predicate<StorageKey> toKeyPredicate() {
    Preconditions.checkNotNull(strategy,
        "Cannot produce a key predicate without a partition strategy");
    return new KeyPredicate(constraints, strategy);
  }

  /**
   * Get a set of {@link MarkerRange} objects that covers the set of possible
   * {@link StorageKey} partitions for this constraint set. If a
   * {@code StorageKey} is not in one of the ranges returned by this method,
   * then its partition cannot contain entities that satisfy this constraint
   * set.
   *
   * @return an Iterable of MarkerRange
   * @throws NullPointerException if no partition strategy is defined
   */
  public Iterable<MarkerRange> toKeyRanges() {
    Preconditions.checkNotNull(strategy,
        "Cannot produce key ranges without a partition strategy");
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
   * @return true if this constraint set is satisfied by partitioning
   * @throws NullPointerException if no partition strategy is defined
   */
  @SuppressWarnings("unchecked")
  public boolean alignedWithBoundaries() {
    Preconditions.checkNotNull(strategy,
        "Cannot produce key ranges without a partition strategy");
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
      if (!(predicate instanceof Exists)) {
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
    SchemaUtil.checkTypeConsistency(schema, strategy, name, values);
    if (values.length > 0) {
      checkContained(name, values);
      // this is the most specific constraint and is idempotent under "and"
      return new Constraints(schema, strategy, constraints, name,
          Predicates.in(values));
    } else {
      if (!constraints.containsKey(name)) {
        // no other constraint => add the exists
        return new Constraints(schema, strategy, constraints, name,
            Predicates.exists());
      } else {
        // satisfied by an existing constraint
        return this;
      }
    }
  }

  public Constraints from(String name, Comparable value) {
    SchemaUtil.checkTypeConsistency(schema, strategy, name, value);
    checkContained(name, value);
    Range added = Ranges.atLeast(value);
    if (constraints.containsKey(name)) {
      return new Constraints(schema, strategy, constraints, name,
          combine(constraints.get(name), added));
    } else {
      return new Constraints(schema, strategy, constraints, name, added);
    }
  }

  public Constraints fromAfter(String name, Comparable value) {
    SchemaUtil.checkTypeConsistency(schema, strategy, name, value);
    checkContained(name, value);
    Range added = Ranges.greaterThan(value);
    if (constraints.containsKey(name)) {
      return new Constraints(schema, strategy, constraints, name,
          combine(constraints.get(name), added));
    } else {
      return new Constraints(schema, strategy, constraints, name, added);
    }
  }

  public Constraints to(String name, Comparable value) {
    SchemaUtil.checkTypeConsistency(schema, strategy, name, value);
    checkContained(name, value);
    Range added = Ranges.atMost(value);
    if (constraints.containsKey(name)) {
      return new Constraints(schema, strategy, constraints, name,
          combine(constraints.get(name), added));
    } else {
      return new Constraints(schema, strategy, constraints, name, added);
    }
  }

  public Constraints toBefore(String name, Comparable value) {
    SchemaUtil.checkTypeConsistency(schema, strategy, name, value);
    checkContained(name, value);
    Range added = Ranges.lessThan(value);
    if (constraints.containsKey(name)) {
      return new Constraints(schema, strategy, constraints, name,
          combine(constraints.get(name), added));
    } else {
      return new Constraints(schema, strategy, constraints, name, added);
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
    out.writeUTF(strategy != null ? strategy.toString() : "");
    ConstraintsSerialization.writeConstraints(schema, strategy, constraints, out);
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
    String json = in.readUTF();
    if (!json.isEmpty()) {
      strategy = PartitionStrategyParser.parse(json);
    }
    constraints = ImmutableMap.copyOf(ConstraintsSerialization.readConstraints(schema, strategy, in));
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
  static Predicate combine(Predicate left, Predicate right) {
    if (left == right) {
      return left;
    } else if (left == null) {
      return right; // must be non-null
    } else if (right == null || right instanceof Exists) {
      return left; // must be non-null, which satisfies exists
    } else if (left instanceof Exists) {
      return right; // must be non-null, which satisfies exists
    } else if (left instanceof In) {
      return ((In) left).filter(right);
    } else if (right instanceof In) {
      return ((In) right).filter(left);
    } else if (left instanceof Range && right instanceof Range) {
      return ((Range) left).intersection((Range) right);
    } else {
      return com.google.common.base.Predicates.and(left, right);
    }
  }

  private static <E> Predicate<E> entityPredicate(
      Map<String, Predicate> predicates, Schema schema,
      PartitionStrategy strategy) {
    if (Schema.Type.RECORD != schema.getType()) {
      return alwaysTrue();
    }
    return new EntityPredicate<E>(predicates, schema, strategy);
  }

  /**
   * Returns true if there are no constraints.
   *
   * @return {@code true} if there are no constraints, {@code false} otherwise
   */
  public boolean isUnbounded() {
    return constraints.isEmpty();
  }

  /**
   * A {@link Predicate} for testing entities against a set of predicates.
   *
   * @param <E> The type of entities this predicate tests
   */
  private static class EntityPredicate<E> implements Predicate<E> {
    private final List<Map.Entry<Schema.Field, Predicate>> predicatesByField;

    @SuppressWarnings("unchecked")
    public EntityPredicate(Map<String, Predicate> predicates, Schema schema,
                           PartitionStrategy strategy) {

      List<Schema.Field> fields = schema.getFields();
      Map<Schema.Field, Predicate> predicateMap = Maps.newHashMap();

      // in the case of identical source and partition names, the predicate
      // will be applied for both source and partition values.

      for (Schema.Field field : fields) {
        Predicate sourcePredicate = predicates.get(field.name());
        if (sourcePredicate != null) {
          predicateMap.put(field, sourcePredicate);
        }
      }

      if (strategy != null) {
        // there could be partition predicates to add
        for (FieldPartitioner fp : strategy.getFieldPartitioners()) {
          Predicate partitionPredicate = predicates.get(fp.getName());
          if (partitionPredicate != null) {
            Predicate transformPredicate = new TransformPredicate(
                fp, partitionPredicate);
            Schema.Field field = schema.getField(fp.getSourceName());
            Predicate sourcePredicate = predicateMap.get(field);
            if (sourcePredicate != null) {
              // combine the source and the transform-wrapped predicates
              predicateMap.put(field,
                  combine(sourcePredicate, transformPredicate));
            } else {
              predicateMap.put(field, transformPredicate);
            }
          }
        }
      }

      this.predicatesByField = ImmutableList.copyOf(predicateMap.entrySet());
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean apply(@Nullable E entity) {
      if (entity == null) {
        return false;
      }

      // check each constraint and fail immediately
      for (Map.Entry<Schema.Field, Predicate> entry : predicatesByField) {
        Object eValue = get(entity, entry.getKey());
        if (!entry.getValue().apply(eValue)) {
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
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      EntityPredicate other = (EntityPredicate) obj;
      return Objects.equal(predicatesByField, other.predicatesByField);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(predicatesByField);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("predicates", predicatesByField)
          .toString();
    }
  }

  private static Object get(Object entity, Schema.Field field) {
    // TODO: this should use the correct Avro data model, not just reflect
    // if this fails to find the field, it throws AvroRuntimeException
    return ReflectData.get().getField(entity, field.name(), field.pos());
  }

  /**
   * A {@link Predicate} for testing a {@link StorageKey} against a set of
   * predicates.
   */
  private static class KeyPredicate implements Predicate<StorageKey> {
    private final List<Predicate> partitionPredicates;
    private final List<Predicate<Marker>> timePredicates;

    @SuppressWarnings("unchecked")
    private KeyPredicate(Map<String, Predicate> predicates,
                         PartitionStrategy strategy) {
      Preconditions.checkNotNull(strategy,
          "Cannot produce KeyPredicate without a PartitionStrategy");

      // in the case of identical source and partition names, there is only one
      // predicate and it is used like normal. the only time this conflicts is
      // when the source and predicate name for a single field are the same, in
      // which case the result will be the projected predicate combined with
      // itself. usually the function is identity when this happens and there is
      // no problem because of the combine identity check.

      List<FieldPartitioner> partitioners = strategy.getFieldPartitioners();
      Predicate[] preds = new Predicate[partitioners.size()];

      Map<String, Predicate> timeFields = Maps.newHashMap();
      for (int i = 0; i < preds.length; i += 1) {
        FieldPartitioner fp = partitioners.get(i);
        Predicate sourcePredicate = predicates.get(fp.getSourceName());
        if (sourcePredicate != null) {
          Predicate projectedPredicate = fp.project(sourcePredicate);
          if (projectedPredicate != null) {
            preds[i] = projectedPredicate;
          }
          if (fp instanceof CalendarFieldPartitioner) {
            timeFields.put(fp.getSourceName(), sourcePredicate);
          }
        }

        Predicate partitionPredicate = predicates.get(fp.getName());
        if (preds[i] != null) {
          if (partitionPredicate != null) {
            preds[i] = combine(partitionPredicate, preds[i]);
          }
        } else {
          if (partitionPredicate != null) {
            preds[i] = partitionPredicate;
          } else {
            preds[i] = alwaysTrue();
          }
        }
      }
      this.partitionPredicates = ImmutableList.copyOf(preds);

      List<Predicate<Marker>> timePreds = Lists.newArrayList();
      for (Map.Entry<String, Predicate> entry : timeFields.entrySet()) {
        timePreds.add(TimeDomain.get(strategy, entry.getKey())
            .project(entry.getValue()));
      }
      this.timePredicates = ImmutableList.copyOf(timePreds);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean apply(StorageKey key) {
      if (key == null) {
        return false;
      }

      // this is fail-fast: if the key fails a constraint, then drop it
      for (int i = 0; i < partitionPredicates.size(); i += 1) {
        Object pValue = key.get(i);
        if (!partitionPredicates.get(i).apply(pValue)) {
          return false;
        }
      }

      for (Predicate<Marker> timePredicate : timePredicates) {
        if (!timePredicate.apply(key)) {
          return false;
        }
      }

      // if we made it this far, everything passed
      return true;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("predicates", partitionPredicates)
          .add("timePredicates", timePredicates)
          .toString();
    }
  }

  /**
   * A Predicate that returns the result of transforming its input and applying
   * a predicate to the transformed value.
   * @param <S> The type of input to this predicate
   * @param <T> The type of input to the wrapped predicate.
   */
  private static class TransformPredicate<S, T> implements Predicate<S> {
    private final Function<S, T> function;
    private final Predicate<T> predicate;

    public TransformPredicate(Function<S, T> function, Predicate<T> predicate) {
      this.function = function;
      this.predicate = predicate;
    }

    @Override
    public boolean apply(@Nullable S input) {
      return predicate.apply(function.apply(input));
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("function", function)
          .add("predicate", predicate)
          .toString();
    }
  }
}
