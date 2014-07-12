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
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.spi.partition.CalendarFieldPartitioner;
import org.kitesdk.data.spi.predicates.In;
import org.kitesdk.data.spi.predicates.Predicates;
import org.kitesdk.data.spi.predicates.Range;

class KeyRangeIterable implements Iterable<MarkerRange> {
  private final Map<String, Predicate> predicates;
  private final PartitionStrategy strategy;
  private final MarkerComparator cmp;

  public KeyRangeIterable(PartitionStrategy strategy, Map<String, Predicate> predicates) {
    this.strategy = strategy;
    this.predicates = predicates;
    this.cmp = new MarkerComparator(strategy);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterator<MarkerRange> iterator() {
    // this should be part of PartitionStrategy
    final LinkedListMultimap<String, FieldPartitioner> partitioners =
        LinkedListMultimap.create();
    for (FieldPartitioner fp : strategy.getFieldPartitioners()) {
      partitioners.put(fp.getSourceName(), fp);
    }

    Iterator<MarkerRange.Builder> current = start(new MarkerRange.Builder(cmp));

    // primarily loop over sources because the logical constraints are there
    for (String source : partitioners.keySet()) {
      Predicate constraint = predicates.get(source);
      List<FieldPartitioner> fps = partitioners.get(source);
      FieldPartitioner first = fps.get(0);
      if (first instanceof CalendarFieldPartitioner) {
        current = TimeDomain.get(strategy, source)
            .addStackedIterator(constraint, current);
      } else if (constraint instanceof In) {
        current = add((In) constraint, fps, current);
      } else if (constraint instanceof Range) {
        current = add((Range) constraint, fps, current);
      }
    }

    return Iterators.transform(current, new ToMarkerRangeFunction());
  }

  private static class ToMarkerRangeFunction implements
      Function<MarkerRange.Builder, MarkerRange> {
    @Override
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
        value="NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE",
        justification="False positive, initialized above as non-null.")
    public MarkerRange apply(@Nullable MarkerRange.Builder builder) {
      return builder.build();
    }
  }

  /**
   * Convenience function to wrap some object in an Iterator.
   *
   * @param singleton an Object to wrap in a singleton iterator
   * @param <T> The type of {@code singleton}
   * @return an Iterator that yeilds the given object once
   */
  private static <T> Iterator<T> start(T singleton) {
    return Collections.singleton(singleton).iterator();
  }

  /**
   * Convenience function to add the correct wrapper {@link StackedIterator} to
   * the key range Iterator stack.
   *
   * This method checks the projected constraints to ensure they are compatible
   * and can be used in a grouped iterator. The constraints that are not are
   * added as a separate {@code StackedIterator}.
   *
   * @param constraint An "in" constraint for the <em>source</em> values
   * @param fps A List of FieldPartitioners with the same source field
   * @param inner An {@code Iterator} to wrap
   * @return A key range Iterator
   */
  @SuppressWarnings("unchecked")
  static Iterator<MarkerRange.Builder> add(
      In constraint, List<FieldPartitioner> fps,
      Iterator<MarkerRange.Builder> inner) {

    Iterator<MarkerRange.Builder> current = inner;
    List<FieldPartitioner> compatible = Lists.newArrayList();
    for (FieldPartitioner fp : fps) {
      Predicate<?> projected = fp.project(constraint);
      if (projected instanceof Range) {
        current = addProjected(projected, fp.getName(), current);
      } else if (projected instanceof In) {
        compatible.add(fp);
      }
      // otherwise, all fields are included, so don't add anything
    }

    if (compatible.size() < 1) {
      return current;
    } else if (compatible.size() == 1) {
      FieldPartitioner fp = compatible.get(0);
      return addProjected(fp.project(constraint), fp.getName(), current);
    } else {
      return new SetGroupIterator(constraint, compatible, current);
    }
  }

  /**
   * Convenience function to add the correct wrapper {@link StackedIterator} to
   * the key range Iterator stack.
   *
   * This method checks the projected constraints to ensure they are compatible
   * and can be used in a grouped iterator. The constraints that are not are
   * added as a separate {@code StackedIterator}. For example, constraints
   * projected by {@link org.kitesdk.data.spi.partition.ListFieldPartitioner} are
   * "in" constraints and can't be grouped with ranges.
   *
   * @param constraint A "range" constraint for the <em>source</em> values
   * @param fps A List of FieldPartitioners with the same source field
   * @param inner An {@code Iterator} to wrap
   * @return A key range Iterator
   */
  @SuppressWarnings("unchecked")
  static Iterator<MarkerRange.Builder> add(
      Range constraint, List<FieldPartitioner> fps,
      Iterator<MarkerRange.Builder> inner) {

    Iterator<MarkerRange.Builder> current = inner;
    List<Pair<String, Range>> compatible = Lists.newArrayList();
    for (FieldPartitioner fp : fps) {
      Predicate<?> projected = fp.project(constraint);
      if (projected instanceof In) {
        current = addProjected(projected, fp.getName(), current);
      } else if (projected instanceof Range) {
        compatible.add(Pair.of(fp.getName(), (Range) projected));
      }
      // otherwise, all fields are included, so don't add anything
    }

    if (compatible.size() < 1) {
      return current;
    } else if (compatible.size() == 1) {
      Pair<String, Range> pair = compatible.get(0);
      return addProjected((Predicate<?>) pair.second(), pair.first(), current);
    } else {
      return new RangeGroupIterator(constraint, compatible, current);
    }
  }

  /**
   * Convenience function to add the correct {@link StackedIterator} to the key
   * range Iterator stack.
   *
   * This method is used to add a <em>single</em> <em>projected</em> constraint
   * to the stack.
   */
  private static Iterator<MarkerRange.Builder> addProjected(
      Predicate projected, String name,
      Iterator<MarkerRange.Builder> inner) {
    if (projected instanceof In) {
      return new SetIterator((In) projected, name, inner);
    } else if (projected instanceof Range) {
      return new RangeIterator(name, (Range) projected, inner);
    } else {
      return inner;
    }
  }

  /**
   * StackedIterator is an abstract {@link Iterator} that helps to implement a
   * cross-product Iterator. Each StackedIterator adds one level of the
   * cross-product, by looping over an internal set for each item in its inner
   * Iterator.
   *
   * For example, a StackedIterator with set ("a", "b") wrapping an Iterator
   * that contains (1, 2, 3) would produce a record for each pair:
   *   (("a", 1), ("b", 1), ("a", 2), ("b", 2), ("a", 3), ("b", 3))
   *
   * Records are created from the inner Iterator's record and an item from the
   * set by the (abstract) {@code update} method.
   *
   * @param <I> The type of items added by this iterator
   * @param <T> The type of container
   */
  abstract static class StackedIterator<I, T> implements Iterator<T> {
    private Iterable<I> items = null;
    private Iterator<I> iterItems = null;
    private Iterator<T> inner = null;
    private T current = null;

    protected final void setItem(I item) {
      setItems(Collections.singleton(item));
    }

    protected final void setItems(Iterable<I> items) {
      this.items = items;
      this.iterItems = items.iterator();
    }

    protected final void setInner(Iterator<T> inner) {
      Preconditions.checkArgument(inner.hasNext(), "Empty iterator");
      this.inner = inner;
      this.current = inner.next();
    }

    @Override
    public boolean hasNext() {
      return iterItems.hasNext() || inner.hasNext();
    }

    @Override
    public T next() {
      if (iterItems.hasNext()) {
        return update(current, iterItems.next());
      } else if (inner.hasNext()) {
        this.current = inner.next();
        this.iterItems = items.iterator();
        return this.next();
      } else {
        throw new NoSuchElementException();
      }
    }

    public abstract T update(T current, I item);

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove is not supported");
    }
  }

  /**
   * A {@link StackedIterator} that multiplies an existing set of key ranges
   * with the possible values from an "in" constraint one partition field.
   *
   * For "in" constraints that affect multiple partition fields, use
   * {@link KeyRangeIterable.SetGroupIterator}.
   */
  static class SetIterator
      extends StackedIterator<Object, MarkerRange.Builder> {
    private final String name;

    /**
     * Construct a {@code SetIterator} for the given <em>projected</em> "in"
     * constraint.
     *
     * @param name The name of the partition field
     * @param projected An "in" constraint of the <em>partition</em> values
     * @param inner An {@code Iterator} to wrap
     */
    @SuppressWarnings("unchecked")
    private SetIterator(In projected, String name,
                       Iterator<MarkerRange.Builder> inner) {
      this.name = name;
      setItems(Predicates.asSet(projected));
      setInner(inner);
    }

    @Override
    public MarkerRange.Builder update(
        MarkerRange.Builder current, Object item) {
      current.addToStart(name, item);
      current.addToEnd(name, item);
      return current;
    }
  }

  /**
   * A {@link StackedIterator} that multiplies an existing set of key ranges
   * with the possible values from an "in" constraint for multiple partition
   * fields.
   *
   * For "in" constraints that affect only one partition field, use
   * {@link KeyRangeIterable.SetIterator}.
   */
  static class SetGroupIterator
      extends StackedIterator<Object, MarkerRange.Builder> {
    private final List<FieldPartitioner> fields;

    /**
     * Construct a {@code SetGroupIterator} for the given <em>source</em> "in"
     * constraint and a set of partition fields that are affected by the source
     * field.
     *
     * Because the possible values for all of the partition fields come from
     * the set of values for the "in" constraint, the number of ranges produced
     * by the final Iterator can be reduced by projecting the partition fields
     * as a group.
     *
     * Example:
     *   partition strategy: hash(field-1) / identity(field-1)
     *   constraints: field-1 in ("a", "b")
     *
     * When using two {@link SetIterator}, this produces pairs:
     *   (hash("a"), "b"), (hash("a"), "a"), (hash("b"), "b"), (hash("b"), "a")
     *
     * When grouped, the correct possible set is produced:
     *   (hash("a"), "a"), (hash("b"), "b")
     *
     * @param constraint An "in" constraint for the <em>source</em> values
     * @param fps A List of FieldPartitioners with the same source field
     * @param inner An {@code Iterator} to wrap
     */
    @SuppressWarnings("unchecked")
    SetGroupIterator(In constraint, List<FieldPartitioner> fps,
                            Iterator<MarkerRange.Builder> inner) {
      this.fields = fps;
      setItems(Predicates.asSet(constraint));
      setInner(inner);
    }

    @Override
    @SuppressWarnings("unchecked")
    public MarkerRange.Builder update(
        MarkerRange.Builder current, Object item) {
      for (FieldPartitioner fp : fields) {
        Object value = fp.apply(item);
        current.addToStart(fp.getName(), value);
        current.addToEnd(fp.getName(), value);
      }
      return current;
    }
  }

  /**
   * A {@link StackedIterator} that adds a "range" constraint to the key ranges.
   *
   * For "range" constraints that affect multiple partition fields, use
   * {@link KeyRangeIterable.RangeGroupIterator}.
   */
  static class RangeIterator
      extends StackedIterator<Range, MarkerRange.Builder> {
    private final String name;
    protected RangeIterator(
        String name, Range range,
        Iterator<MarkerRange.Builder> inner) {
      this.name = name;
      setItem(range);
      setInner(inner);
    }

    @Override
    public MarkerRange.Builder update(
        MarkerRange.Builder current, Range range) {
      if (range.hasLowerBound()) {
        current.addToStart(name, range.lowerEndpoint());
      }
      if (range.hasUpperBound()) {
        current.addToEnd(name, range.upperEndpoint());
      }
      return current;
    }
  }

  /**
   * A {@link StackedIterator} that adds a group of "range" constraint to the
   * key ranges.
   *
   * For "range" constraints that affect only one partition field, use
   * {@link KeyRangeIterable.RangeIterator}.
   */
  static class RangeGroupIterator
      extends StackedIterator<Range, MarkerRange.Builder> {
    private final List<Pair<String, Range>> fields;

    protected RangeGroupIterator(Range constraint, List<Pair<String, Range>> compatible,
                              Iterator<MarkerRange.Builder> inner) {
      this.fields = compatible;
      setItem(constraint);
      setInner(inner);
    }

    @Override
    public MarkerRange.Builder update(
        MarkerRange.Builder current, Range range) {
      if (range.hasLowerBound()) {
        for (Pair<String, Range> pair : fields) {
          current.addToStart(pair.first(), pair.second().lowerEndpoint());
        }
      }
      if (range.hasUpperBound()) {
        for (Pair<String, Range> pair : fields) {
          current.addToEnd(pair.first(), pair.second().upperEndpoint());
        }
      }
      return current;
    }
  }
}
