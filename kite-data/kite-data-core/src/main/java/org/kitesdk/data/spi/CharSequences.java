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

import com.google.common.collect.HashMultimap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;

class CharSequences {

  public static int compare(CharSequence left, CharSequence right) {
    if (left == right) {
      return 0;
    }
    int minLength = Math.min(left.length(), right.length());
    for (int i = 0; i < minLength; i += 1) {
      int cmp = Character.compare(left.charAt(i), right.charAt(i));
      if (cmp != 0) {
        return cmp;
      }
    }
    return Integer.compare(left.length(), right.length());
  }

  public static int hashCode(CharSequence seq) {
    final int prime = 37;
    int result = 13;
    for (int i = 0, n = seq.length(); i < n; i += 1) {
      result = prime * result + seq.charAt(i);
    }
    return result;
  }

  public static class CharSequenceSet extends AbstractSet<CharSequence> {
    private final HashMultimap<Integer, CharSequence> storage = HashMultimap.create();
    private final int size;

    public CharSequenceSet(Iterable<CharSequence> strings) {
      int count = 0;
      for (CharSequence seq : strings) {
        storage.put(CharSequences.hashCode(seq), seq);
        count += 1;
      }
      this.size = count;
    }

    @Override
    public boolean contains(Object query) {
      if (query instanceof CharSequence) {
        CharSequence seq = (CharSequence) query;
        Set<CharSequence> set = storage.get(CharSequences.hashCode(seq));
        for (CharSequence next : set) {
          if (compare(seq, next) == 0) {
            return true;
          }
        }
      }
      return false;
    }

    @Override
    public Iterator<CharSequence> iterator() {
      return storage.values().iterator();
    }

    @Override
    public int size() {
      return size;
    }
  }
}
