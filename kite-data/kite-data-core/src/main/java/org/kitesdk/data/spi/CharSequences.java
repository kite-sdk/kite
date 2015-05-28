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

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedHashMultimap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;
import javax.annotation.concurrent.Immutable;

public class CharSequences {

  public static int compare(CharSequence left, CharSequence right) {
    if (left == right) {
      return 0;
    }
    int minLength = Math.min(left.length(), right.length());
    for (int i = 0; i < minLength; i += 1) {
      int cmp = (left.charAt(i) - right.charAt(i));
      if (cmp != 0) {
        return cmp;
      }
    }
    return (left.length() - right.length());
  }

  public static int hashCode(CharSequence seq) {
    final int prime = 37;
    int result = 13;
    for (int i = 0, n = seq.length(); i < n; i += 1) {
      result = prime * result + seq.charAt(i);
    }
    return result;
  }

  @Immutable
  public static class ImmutableCharSequenceSet extends AbstractSet<CharSequence> {
    private final LinkedHashMultimap<Integer, CharSequence> storage =
        LinkedHashMultimap.create();
    private final int size;

    public ImmutableCharSequenceSet(Object... strings) {
      int count = 0;
      for (Object obj : strings) {
        CharSequence seq = (CharSequence) obj;
        // like guava collections, do not allow null
        Preconditions.checkNotNull(seq, "Null values are not allowed");
        if (!contains(seq)) { // don't add duplicates
          storage.put(CharSequences.hashCode(seq), seq);
          count += 1;
        }
      }
      this.size = count;
    }

    public ImmutableCharSequenceSet(Iterable<? extends CharSequence> strings) {
      int count = 0;
      for (CharSequence seq : strings) {
        // like guava collections, do not allow null
        Preconditions.checkNotNull(seq, "Null values are not allowed");
        if (!contains(seq)) { // don't add duplicates
          storage.put(CharSequences.hashCode(seq), seq);
          count += 1;
        }
      }
      this.size = count;
    }

    @Override
    public boolean add(CharSequence charSequence) {
      throw new UnsupportedOperationException("Cannot add to Immutable set");
    }

    @Override
    public boolean remove(Object o) {
      throw new UnsupportedOperationException("Cannot remove from Immutable set");
    }

    @Override
    public boolean contains(Object query) {
      return query instanceof CharSequence && contains((CharSequence) query);
    }

    @Override
    public Iterator<CharSequence> iterator() {
      return storage.values().iterator();
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public boolean equals(Object o) {
      // satisfy findbugs. AbstractSet#equals is correct and based on contains
      return super.equals(o);
    }

    @Override
    public int hashCode() {
      int hashCode = 0;
      // like AbstractSet, hashCode is the sum of the contained seq hashCodes
      for (CharSequence seq : this) {
        hashCode += CharSequences.hashCode(seq);
      }
      return hashCode;
    }

    private boolean contains(CharSequence seq) {
      Set<CharSequence> set = storage.get(CharSequences.hashCode(seq));
      for (CharSequence next : set) {
        if (compare(seq, next) == 0) {
          return true;
        }
      }
      return false;
    }
  }
}
