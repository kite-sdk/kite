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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.data.TestHelpers;

import static org.kitesdk.data.spi.CharSequences.ImmutableCharSequenceSet;

public class TestCharSequences {

  @Test
  public void testCompareStartsWith() {
    Utf8 first = new Utf8("abcd");
    Utf8 second = new Utf8("abc");

    // first <=> second
    int cmp = first.compareTo(second);
    Assert.assertEquals("Utf8 and String compareTo should agree",
        cmp, first.toString().compareTo(second.toString()));
    Assert.assertEquals("compare(Utf8,Utf8)",
        cmp, CharSequences.compare(first, second));
    Assert.assertEquals("compare(String,Utf8)",
        cmp, CharSequences.compare(first.toString(), second));
    Assert.assertEquals("compare(Utf8,String)",
        cmp, CharSequences.compare(first, second.toString()));
    Assert.assertEquals("compare(String,String)",
        cmp, CharSequences.compare(first.toString(), second.toString()));

    // second <=> first
    cmp = second.compareTo(first);
    Assert.assertEquals("Utf8 and String compareTo should agree",
        cmp, second.toString().compareTo(first.toString()));
    Assert.assertEquals("compare(Utf8,Utf8)",
        cmp, CharSequences.compare(second, first));
    Assert.assertEquals("compare(String,Utf8)",
        cmp, CharSequences.compare(second.toString(), first));
    Assert.assertEquals("compare(Utf8,String)",
        cmp, CharSequences.compare(second, first.toString()));
    Assert.assertEquals("compare(String,String)",
        cmp, CharSequences.compare(second.toString(), first.toString()));
  }

  @Test
  public void testCompareDifferBeforeEnd() {
    Utf8 first = new Utf8("abcd");
    Utf8 second = new Utf8("abd");

    // first <=> second
    int cmp = first.compareTo(second);
    Assert.assertEquals("Utf8 and String compareTo should agree",
        cmp, first.toString().compareTo(second.toString()));
    Assert.assertEquals("compare(Utf8,Utf8)",
        cmp, CharSequences.compare(first, second));
    Assert.assertEquals("compare(String,Utf8)",
        cmp, CharSequences.compare(first.toString(), second));
    Assert.assertEquals("compare(Utf8,String)",
        cmp, CharSequences.compare(first, second.toString()));
    Assert.assertEquals("compare(String,String)",
        cmp, CharSequences.compare(first.toString(), second.toString()));

    // second <=> first
    cmp = second.compareTo(first);
    Assert.assertEquals("Utf8 and String compareTo should agree",
        cmp, second.toString().compareTo(first.toString()));
    Assert.assertEquals("compare(Utf8,Utf8)",
        cmp, CharSequences.compare(second, first));
    Assert.assertEquals("compare(String,Utf8)",
        cmp, CharSequences.compare(second.toString(), first));
    Assert.assertEquals("compare(Utf8,String)",
        cmp, CharSequences.compare(second, first.toString()));
    Assert.assertEquals("compare(String,String)",
        cmp, CharSequences.compare(second.toString(), first.toString()));
  }

  @Test
  public void testCompareEqualLength() {
    Utf8 first = new Utf8("abcd");
    Utf8 second = new Utf8("abdd");

    // first <=> first
    int cmp = first.compareTo(first);
    Assert.assertEquals("Utf8 and String compareTo should agree",
        cmp, first.toString().compareTo(first.toString()));
    Assert.assertEquals("compare(Utf8,Utf8)", // hits (left == right) check
        cmp, CharSequences.compare(first, first));
    Assert.assertEquals("compare(Utf8,Utf8)", // force (left != right)
        cmp, CharSequences.compare(first, new Utf8(first)));
    Assert.assertEquals("compare(String,Utf8)",
        cmp, CharSequences.compare(first.toString(), first));
    Assert.assertEquals("compare(Utf8,String)",
        cmp, CharSequences.compare(first, first.toString()));
    Assert.assertEquals("compare(String,String)",
        cmp, CharSequences.compare(first.toString(), first.toString()));

    // first <=> second
    cmp = first.compareTo(second);
    Assert.assertEquals("Utf8 and String compareTo should agree",
        cmp, first.toString().compareTo(second.toString()));
    Assert.assertEquals("compare(Utf8,Utf8)",
        cmp, CharSequences.compare(first, second));
    Assert.assertEquals("compare(String,Utf8)",
        cmp, CharSequences.compare(first.toString(), second));
    Assert.assertEquals("compare(Utf8,String)",
        cmp, CharSequences.compare(first, second.toString()));
    Assert.assertEquals("compare(String,String)",
        cmp, CharSequences.compare(first.toString(), second.toString()));

    // second <=> first
    cmp = second.compareTo(first);
    Assert.assertEquals("Utf8 and String compareTo should agree",
        cmp, second.toString().compareTo(first.toString()));
    Assert.assertEquals("compare(Utf8,Utf8)",
        cmp, CharSequences.compare(second, first));
    Assert.assertEquals("compare(String,Utf8)",
        cmp, CharSequences.compare(second.toString(), first));
    Assert.assertEquals("compare(Utf8,String)",
        cmp, CharSequences.compare(second, first.toString()));
    Assert.assertEquals("compare(String,String)",
        cmp, CharSequences.compare(second.toString(), first.toString()));
  }

  @Test
  public void testHashCode() {
    Utf8 first = new Utf8("abcd");
    Utf8 second = new Utf8("abdd");

    Assert.assertEquals("hashCode(Utf8) == hashCode(Utf8)",
        CharSequences.hashCode(first),
        CharSequences.hashCode(new Utf8(first)));
    Assert.assertEquals("hashCode(Utf8) == hashCode(String)",
        CharSequences.hashCode(first),
        CharSequences.hashCode(first.toString()));
    Assert.assertEquals("hashCode(String) == hashCode(String)",
        CharSequences.hashCode(first.toString()),
        CharSequences.hashCode(first.toString()));
    Assert.assertEquals("hashCode(Utf8) == hashCode(Utf8)",
        CharSequences.hashCode(second),
        CharSequences.hashCode(new Utf8(second)));
    Assert.assertEquals("hashCode(Utf8) == hashCode(String)",
        CharSequences.hashCode(second),
        CharSequences.hashCode(second.toString()));
    Assert.assertEquals("hashCode(String) == hashCode(String)",
        CharSequences.hashCode(second.toString()),
        CharSequences.hashCode(second.toString()));
    // not always true, but usually is
    Assert.assertFalse("(a != b) => hashCode(a) != hashCode(b) (mostly)",
        CharSequences.hashCode(first) == CharSequences.hashCode(second));
  }

  @Test
  public void testStringCharSequenceSetContains() {
    List<String> colors = Lists.newArrayList(
        "orange", "green", "blue", "red", "purple", "red");
    ImmutableCharSequenceSet set = new ImmutableCharSequenceSet(colors);

    for (String color : colors) {
      Assert.assertTrue("Should contain Utf8(value)",
          set.contains(new Utf8(color)));
    }
    Assert.assertTrue("Should contain all values",
        set.containsAll(colors));
  }

  @Test
  public void testUtf8CharSequenceSetContains() {
    List<Utf8> colors = Lists.newArrayList(
        new Utf8("orange"), new Utf8("green"), new Utf8("blue"),
        new Utf8("red"), new Utf8("purple"), new Utf8("red"));
    ImmutableCharSequenceSet set = new ImmutableCharSequenceSet(colors);

    for (Utf8 color : colors) {
      Assert.assertTrue("Should contain Utf8(value)",
          set.contains(color.toString()));
    }
    Assert.assertTrue("Should contain all values",
        set.containsAll(colors));
  }

  @Test
  public void testStringCharSequenceSetSize() {
    List<String> colors = Lists.newArrayList(
        "orange", "green", "blue", "red", "purple", "red");
    ImmutableCharSequenceSet set = new ImmutableCharSequenceSet(colors);

    Assert.assertEquals("Should remove duplicate color", 5, set.size());
  }

  @Test
  public void testUtf8CharSequenceSetSize() {
    List<Utf8> colors = Lists.newArrayList(
        new Utf8("orange"), new Utf8("green"), new Utf8("blue"),
        new Utf8("red"), new Utf8("purple"), new Utf8("red"));
    ImmutableCharSequenceSet set = new ImmutableCharSequenceSet(colors);

    Assert.assertEquals("Should remove duplicate color", 5, set.size());
  }

  @Test
  public void testStringCharSequenceSetIterator() {
    List<String> colors = Lists.newArrayList(
        "orange", "green", "blue", "red", "purple", "red");
    Set<String> normalSet = Sets.newHashSet(colors);
    ImmutableCharSequenceSet set = new ImmutableCharSequenceSet(colors);
    for (CharSequence color : set) {
      Assert.assertTrue("Should return original types", color instanceof String);
      normalSet.remove(color);
    }
    Assert.assertTrue("Should iterate through all values", normalSet.isEmpty());
  }

  @Test
  public void testUtf8CharSequenceSetIterator() {
    List<Utf8> colors = Lists.newArrayList(
        new Utf8("orange"), new Utf8("green"), new Utf8("blue"),
        new Utf8("red"), new Utf8("purple"), new Utf8("red"));
    Set<Utf8> normalSet = Sets.newHashSet(colors);
    ImmutableCharSequenceSet set = new ImmutableCharSequenceSet(colors);
    for (CharSequence color : set) {
      Assert.assertTrue("Should return original types", color instanceof Utf8);
      normalSet.remove(color);
    }
    Assert.assertTrue("Should iterate through all values", normalSet.isEmpty());
  }

  @Test
  public void testStringCharSequenceSetImmutable() {
    List<String> colors = Lists.newArrayList(
        "orange", "green", "blue", "red", "purple", "red");
    final ImmutableCharSequenceSet set = new ImmutableCharSequenceSet(colors);
    TestHelpers.assertThrows("Should reject additions to the set",
        UnsupportedOperationException.class, new Runnable() {
          @Override
          public void run() {
            set.add("white");
          }
        });
    TestHelpers.assertThrows("Should reject bulk additions to the set",
        UnsupportedOperationException.class, new Runnable() {
          @Override
          public void run() {
            set.addAll(Lists.newArrayList("white", "black"));
          }
        });
    TestHelpers.assertThrows("Should reject removals from the set",
        UnsupportedOperationException.class, new Runnable() {
          @Override
          public void run() {
            set.remove("purple");
          }
        });
    TestHelpers.assertThrows("Should reject bulk removals from the set",
        UnsupportedOperationException.class, new Runnable() {
          @Override
          public void run() {
            set.removeAll(Lists.newArrayList("purple", "red"));
          }
        });
  }

  @Test
  public void testUtf8CharSequenceSetImmutable() {
    List<Utf8> colors = Lists.newArrayList(
        new Utf8("orange"), new Utf8("green"), new Utf8("blue"),
        new Utf8("red"), new Utf8("purple"), new Utf8("red"));
    final ImmutableCharSequenceSet set = new ImmutableCharSequenceSet(colors);
    TestHelpers.assertThrows("Should reject additions to the set",
        UnsupportedOperationException.class, new Runnable() {
          @Override
          public void run() {
            set.add(new Utf8("white"));
          }
        });
    TestHelpers.assertThrows("Should reject bulk additions to the set",
        UnsupportedOperationException.class, new Runnable() {
          @Override
          public void run() {
            set.addAll(Lists.newArrayList(new Utf8("white"), new Utf8("black")));
          }
        });
    TestHelpers.assertThrows("Should reject removals from the set",
        UnsupportedOperationException.class, new Runnable() {
          @Override
          public void run() {
            set.remove(new Utf8("purple"));
          }
        });
    TestHelpers.assertThrows("Should reject bulk removals from the set",
        UnsupportedOperationException.class, new Runnable() {
          @Override
          public void run() {
            set.removeAll(Lists.newArrayList(new Utf8("purple"), new Utf8("red")));
          }
        });
  }

  @Test
  public void testCharSequenceSetEqualsAndHashCode() {
    List<String> stringColors = Lists.newArrayList(
        "orange", "green", "blue", "red", "purple", "red");
    ImmutableCharSequenceSet strings = new ImmutableCharSequenceSet(stringColors);

    List<Utf8> utf8Colors = Lists.newArrayList(
        new Utf8("orange"), new Utf8("green"), new Utf8("blue"),
        new Utf8("red"), new Utf8("purple"), new Utf8("red"));
    ImmutableCharSequenceSet utf8s = new ImmutableCharSequenceSet(utf8Colors);

    Assert.assertEquals("Identical String and Utf8 sets should be equal",
        strings, utf8s);
    Assert.assertEquals("Sets equal => hash codes equal",
        strings.hashCode(), utf8s.hashCode());
  }
}
