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

package org.kitesdk.data.spi.filesystem;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterates through all entries at a certain depth of a recursive structure.
 *
 * Each entry returned by {@link #next()} is represented by a List of entries
 * in each level of the structure, up to depth. For example, in a multi-level
 * HashMap:
 *   { "a": { "1": {},
 *            "2": {} },
 *     "b": { "3": { "X": {} } },
 *     "c": {} }
 *
 * Entries of depth 2 are:
 *   [ "a", "1" ]
 *   [ "a", "2" ]
 *   [ "b", "3" ]
 *
 * Entries of depth 3 are:
 *   [ "b", "3", "X" ]
 *
 * This effectively flattens a recursive structure to make tasks easier.
 */
abstract class MultiLevelIterator<T> implements Iterator<List<T>>, Iterable<List<T>> {

  public static final Logger LOG = LoggerFactory.getLogger(MultiLevelIterator.class);

  private final int depth;
  private final LinkedList<T> current;
  private final LinkedList<Iterator<T>> iterators;
  private boolean initialized = false;
  private boolean hasNext = false;

  public MultiLevelIterator(int depth) {
    Preconditions.checkArgument(depth > 0, "Depth must be > 0");
    this.depth = depth;
    this.current = Lists.newLinkedList();
    this.iterators = Lists.newLinkedList();
    this.initialized = false;
  }

  /**
   * Returns an {@code Iterable} of entries at the current path within a
   * recursive structure.
   *
   * For example, in a file system, {@current current} represents a path and
   * the returned {@code Iterable} should enumerate the file and folder names
   * that are direct children of that path.
   *
   * @param current The current partial structure
   * @return An Iterator for the direct children of current in the structure
   */
  public abstract Iterable<T> getLevel(List<T> current);

  private void init() {
    iterators.add(getLevel(current).iterator());
    this.hasNext = advance();
  }

  @Override
  public boolean hasNext() {
    if (!initialized) {
      initialized = true;
      init();
    }
    return hasNext;
  }

  @Override
  public List<T> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    current.add(iterators.getLast().next());
    List<T> copy = ImmutableList.copyOf(current);
    current.removeLast();

    // advance the iterator stack so it is ready for the next
    this.hasNext = advance();

    return copy;
  }

  /**
   * Advance the iterator stack to the next item or return false if there are
   * none left.
   *
   * If this function returns true, then there are {@code depth} iterators in
   * the iterator stack and {@code hasNext()} is true for the last iterator.
   *
   * If this function returns false, then there are no more items in the multi-
   * level iterator. The top-most iterator, and the tree under it, have been
   * exhausted.
   *
   * @return true if there is a next item, false if this iterator is exhausted
   */
  private boolean advance() {
    // done when there are depth iterators and the last iterator has an item
    while (iterators.size() < depth || !iterators.getLast().hasNext()) {
      // each iteration: add an iterator for the next level from the current
      // last iterator, or remove the last iterator because it is empty
      if (iterators.getLast().hasNext()) {
        current.add(iterators.getLast().next());
        iterators.add(getLevel(current).iterator());
      } else {
        iterators.removeLast();
        if (iterators.isEmpty()) {
          return false;
        } else {
          current.removeLast();
        }
      }
    }

    return true;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public Iterator<List<T>> iterator() {
    return this;
  }

}
