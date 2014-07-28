/*
 * Copyright 201"ab" Cloudera Inc.
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

import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.Test;

public class TestRangeCharSequence {
  @Test
  public void testStringRangeAcceptsUtf8() {
    Range<CharSequence> range = Ranges.<CharSequence>openClosed("ab", "cd");
    Assert.assertEquals(range.toString(), "(ab, cd]");
    Assert.assertTrue("Should contain inner Utf8",
        range.contains(new Utf8("ac")));
    Assert.assertFalse("Should not contain outside Utf8",
        range.contains(new Utf8("ab")));
  }

  @Test
  public void testUtf8RangeAcceptsString() {
    Range<CharSequence> range = Ranges.<CharSequence>
        openClosed(new Utf8("ab"), new Utf8("cd"));
    Assert.assertEquals(range.toString(), "(ab, cd]");
    Assert.assertTrue("Should contain inner Utf8",
        range.contains("ac"));
    Assert.assertFalse("Should not contain outside Utf8",
        range.contains("ab"));
  }

  @Test
  public void testSingletonRangeComparable() {
    Range<String> range = Ranges.singleton("ab");
    Assert.assertEquals(range.toString(), "[ab, ab]");
    Assert.assertTrue("Should contain inner point", range.contains("ab"));
    Assert.assertFalse("Should not contain outside point", range.contains("aa"));
  }

  @Test
  public void testOpenRangeComparable() {
    Range<String> range = Ranges.open("ab", "cd");
    Assert.assertEquals(range.toString(), "(ab, cd)");
    Assert.assertTrue("Should contain inner point", range.contains("b"));
    Assert.assertFalse("Should not contain outside point", range.contains("aa"));
    Assert.assertFalse("Should not contain lower endpoint", range.contains("ab"));
    Assert.assertFalse("Should not contain upper endpoint", range.contains("cd"));
  }

  @Test
  public void testClosedRangeComparable() {
    Range<String> range = Ranges.closed("ab", "cd");
    Assert.assertEquals(range.toString(), "[ab, cd]");
    Assert.assertTrue("Should contain inner point", range.contains("b"));
    Assert.assertFalse("Should not contain outside point", range.contains("aa"));
    Assert.assertTrue("Should contain lower endpoint", range.contains("ab"));
    Assert.assertTrue("Should contain upper endpoint", range.contains("cd"));
  }

  @Test
  public void testOpenClosedRangeComparable() {
    Range<String> range = Ranges.openClosed("ab", "cd");
    Assert.assertEquals(range.toString(), "(ab, cd]");
    Assert.assertTrue("Should contain inner point", range.contains("b"));
    Assert.assertFalse("Should not contain outside point", range.contains("aa"));
    Assert.assertFalse("Should not contain lower endpoint", range.contains("ab"));
    Assert.assertTrue("Should contain upper endpoint", range.contains("cd"));
  }

  @Test
  public void testClosedOpenRangeComparable() {
    Range<String> range = Ranges.closedOpen("ab", "cd");
    Assert.assertEquals(range.toString(), "[ab, cd)");
    Assert.assertTrue("Should contain inner point", range.contains("b"));
    Assert.assertFalse("Should not contain outside point", range.contains("aa"));
    Assert.assertTrue("Should contain lower endpoint", range.contains("ab"));
    Assert.assertFalse("Should not contain upper endpoint", range.contains("cd"));
  }

  @Test
  public void testAtLeastRangeComparable() {
    Range<String> range = Ranges.atLeast("ab");
    Assert.assertEquals(range.toString(), "[ab, inf)");
    Assert.assertTrue("Should contain inner point", range.contains("b"));
    Assert.assertFalse("Should not contain outside point", range.contains("aa"));
    Assert.assertTrue("Should contain lower endpoint", range.contains("ab"));
    Assert.assertTrue("Should contain max", range.contains("zzzzzzz"));
  }

  @Test
  public void testGreaterThanRangeComparable() {
    Range<String> range = Ranges.greaterThan("ab");
    Assert.assertEquals(range.toString(), "(ab, inf)");
    Assert.assertTrue("Should contain inner point", range.contains("b"));
    Assert.assertFalse("Should not contain outside point", range.contains("aa"));
    Assert.assertFalse("Should not contain lower endpoint", range.contains("ab"));
    Assert.assertTrue("Should contain max", range.contains("zzzzzzz"));
  }

  @Test
  public void testAtMostRangeComparable() {
    Range<String> range = Ranges.atMost("cd");
    Assert.assertEquals(range.toString(), "(inf, cd]");
    Assert.assertTrue("Should contain inner point", range.contains("b"));
    Assert.assertFalse("Should not contain outside point", range.contains("d"));
    Assert.assertTrue("Should contain min", range.contains(""));
    Assert.assertTrue("Should contain upper endpoint", range.contains("cd"));
  }

  @Test
  public void testLessThanRangeComparable() {
    Range<String> range = Ranges.lessThan("cd");
    Assert.assertEquals(range.toString(), "(inf, cd)");
    Assert.assertTrue("Should contain inner point", range.contains("b"));
    Assert.assertFalse("Should not contain outside point", range.contains("d"));
    Assert.assertTrue("Should contain min", range.contains(""));
    Assert.assertFalse("Should not contain upper endpoint", range.contains("cd"));
  }

  @Test
  public void testAsGuavaRange() {
    // some methods like asSet convert to Guava rather than reimplementing
    Assert.assertEquals("Should convert to guava singleton",
        com.google.common.collect.Ranges.singleton("ab"),
        Ranges.asGuavaRange(Ranges.singleton("ab")));
    Assert.assertEquals("Should convert to guava open",
        com.google.common.collect.Ranges.open("ab", "cd"),
        Ranges.asGuavaRange(Ranges.open("ab", "cd")));
    Assert.assertEquals("Should convert to guava closed",
        com.google.common.collect.Ranges.closed("ab", "cd"),
        Ranges.asGuavaRange(Ranges.closed("ab", "cd")));
    Assert.assertEquals("Should convert to guava openClosed",
        com.google.common.collect.Ranges.openClosed("ab", "cd"),
        Ranges.asGuavaRange(Ranges.openClosed("ab", "cd")));
    Assert.assertEquals("Should convert to guava closedOpen",
        com.google.common.collect.Ranges.closedOpen("ab", "cd"),
        Ranges.asGuavaRange(Ranges.closedOpen("ab", "cd")));
    Assert.assertEquals("Should convert to guava atLeast",
        com.google.common.collect.Ranges.atLeast("ab"),
        Ranges.asGuavaRange(Ranges.atLeast("ab")));
    Assert.assertEquals("Should convert to guava greaterThan",
        com.google.common.collect.Ranges.greaterThan("ab"),
        Ranges.asGuavaRange(Ranges.greaterThan("ab")));
    Assert.assertEquals("Should convert to guava atMost",
        com.google.common.collect.Ranges.atMost("cd"),
        Ranges.asGuavaRange(Ranges.atMost("cd")));
    Assert.assertEquals("Should convert to guava lessThan",
        com.google.common.collect.Ranges.lessThan("cd"),
        Ranges.asGuavaRange(Ranges.lessThan("cd")));
  }

  @Test
  public void testIntersectionOverlap() {
    Assert.assertEquals("Open inner endpoints remain open",
        Ranges.open("d", "dz"),
        Ranges.closedOpen("cd", "dz").intersection(Ranges.openClosed("d", "ee")));
    Assert.assertEquals("Open inner endpoints remain open",
        Ranges.open("d", "dz"),
        Ranges.openClosed("d", "ee").intersection(Ranges.closedOpen("cd", "dz")));

    Assert.assertEquals("Closed inner endpoints remain closed",
        Ranges.closed("d", "dz"),
        Ranges.openClosed("cd", "dz").intersection(Ranges.closedOpen("d", "ee")));
    Assert.assertEquals("Closed inner endpoints remain closed",
        Ranges.closed("d", "dz"),
        Ranges.closedOpen("d", "ee").intersection(Ranges.openClosed("cd", "dz")));

    Assert.assertEquals("Start endpoints equal, one open",
        Ranges.openClosed("cd", "dz"),
        Ranges.openClosed("cd", "dz").intersection(Ranges.closedOpen("cd", "ee")));
    Assert.assertEquals("Start endpoints equal, one open",
        Ranges.openClosed("cd", "dz"),
        Ranges.closedOpen("cd", "ee").intersection(Ranges.openClosed("cd", "dz")));

    Assert.assertEquals("Start endpoints equal, both closed",
        Ranges.closedOpen("cd", "dz"),
        Ranges.closedOpen("cd", "dz").intersection(Ranges.closedOpen("cd", "ee")));
    Assert.assertEquals("Start endpoints equal, both closed",
        Ranges.closedOpen("cd", "dz"),
        Ranges.closedOpen("cd", "ee").intersection(Ranges.closedOpen("cd", "dz")));

    Assert.assertEquals("Stop endpoints equal, one open",
        Ranges.open("d", "ee"),
        Ranges.openClosed("d", "ee").intersection(Ranges.closedOpen("cd", "ee")));
    Assert.assertEquals("Stop endpoints equal, one open",
        Ranges.open("d", "ee"),
        Ranges.closedOpen("cd", "ee").intersection(Ranges.openClosed("d", "ee")));

    Assert.assertEquals("Stop endpoints equal, both closed",
        Ranges.openClosed("d", "ee"),
        Ranges.openClosed("d", "ee").intersection(Ranges.openClosed("cd", "ee")));
    Assert.assertEquals("Stop endpoints equal, both closed",
        Ranges.openClosed("d", "ee"),
        Ranges.openClosed("cd", "ee").intersection(Ranges.openClosed("d", "ee")));
  }

  @Test
  public void testIntersectionPositiveInfinity() {
    Assert.assertEquals("Defined endpoint is used",
        Ranges.closed("d", "ee"),
        Ranges.openClosed("cd", "ee").intersection(Ranges.atLeast("d")));
    Assert.assertEquals("Defined endpoint is used",
        Ranges.closed("d", "ee"),
        Ranges.atLeast("d").intersection(Ranges.openClosed("cd", "ee")));
    Assert.assertEquals("Defined endpoint is used",
        Ranges.open("d", "ee"),
        Ranges.closedOpen("cd", "ee").intersection(Ranges.greaterThan("d")));
    Assert.assertEquals("Defined endpoint is used",
        Ranges.open("d", "ee"),
        Ranges.greaterThan("d").intersection(Ranges.closedOpen("cd", "ee")));
  }

  @Test
  public void testIntersectionNegativeInfinity() {
    Assert.assertEquals("Defined endpoint is used",
        Ranges.openClosed("cd", "d"),
        Ranges.openClosed("cd", "ee").intersection(Ranges.atMost("d")));
    Assert.assertEquals("Defined endpoint is used",
        Ranges.openClosed("cd", "d"),
        Ranges.atMost("d").intersection(Ranges.openClosed("cd", "ee")));
    Assert.assertEquals("Defined endpoint is used",
        Ranges.closedOpen("cd", "d"),
        Ranges.closedOpen("cd", "ee").intersection(Ranges.lessThan("d")));
    Assert.assertEquals("Defined endpoint is used",
        Ranges.closedOpen("cd", "d"),
        Ranges.lessThan("d").intersection(Ranges.closedOpen("cd", "ee")));
  }
}
