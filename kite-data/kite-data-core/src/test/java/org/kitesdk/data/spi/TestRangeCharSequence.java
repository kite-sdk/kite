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

package org.kitesdk.data.spi;

import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.Test;

public class TestRangeCharSequence {
  @Test
  public void testStringRangeAcceptsUtf8() {
    Range<CharSequence> range = Range.<CharSequence>openClosed("ab", "cd");
    Assert.assertEquals(range.toString(), "(ab, cd]");
    Assert.assertTrue("Should contain inner Utf8",
        range.contains(new Utf8("ac")));
    Assert.assertFalse("Should not contain outside Utf8",
        range.contains(new Utf8("ab")));
  }

  @Test
  public void testUtf8RangeAcceptsString() {
    Range<CharSequence> range = Range.<CharSequence>
        openClosed(new Utf8("ab"), new Utf8("cd"));
    Assert.assertEquals(range.toString(), "(ab, cd]");
    Assert.assertTrue("Should contain inner Utf8",
        range.contains("ac"));
    Assert.assertFalse("Should not contain outside Utf8",
        range.contains("ab"));
  }

  @Test
  public void testSingletonRangeComparable() {
    Range<String> range = Range.singleton("ab");
    Assert.assertEquals(range.toString(), "[ab, ab]");
    Assert.assertTrue("Should contain inner point", range.contains("ab"));
    Assert.assertFalse("Should not contain outside point", range.contains("aa"));
  }

  @Test
  public void testOpenRangeComparable() {
    Range<String> range = Range.open("ab", "cd");
    Assert.assertEquals(range.toString(), "(ab, cd)");
    Assert.assertTrue("Should contain inner point", range.contains("b"));
    Assert.assertFalse("Should not contain outside point", range.contains("aa"));
    Assert.assertFalse("Should not contain lower endpoint", range.contains("ab"));
    Assert.assertFalse("Should not contain upper endpoint", range.contains("cd"));
  }

  @Test
  public void testClosedRangeComparable() {
    Range<String> range = Range.closed("ab", "cd");
    Assert.assertEquals(range.toString(), "[ab, cd]");
    Assert.assertTrue("Should contain inner point", range.contains("b"));
    Assert.assertFalse("Should not contain outside point", range.contains("aa"));
    Assert.assertTrue("Should contain lower endpoint", range.contains("ab"));
    Assert.assertTrue("Should contain upper endpoint", range.contains("cd"));
  }

  @Test
  public void testOpenClosedRangeComparable() {
    Range<String> range = Range.openClosed("ab", "cd");
    Assert.assertEquals(range.toString(), "(ab, cd]");
    Assert.assertTrue("Should contain inner point", range.contains("b"));
    Assert.assertFalse("Should not contain outside point", range.contains("aa"));
    Assert.assertFalse("Should not contain lower endpoint", range.contains("ab"));
    Assert.assertTrue("Should contain upper endpoint", range.contains("cd"));
  }

  @Test
  public void testClosedOpenRangeComparable() {
    Range<String> range = Range.closedOpen("ab", "cd");
    Assert.assertEquals(range.toString(), "[ab, cd)");
    Assert.assertTrue("Should contain inner point", range.contains("b"));
    Assert.assertFalse("Should not contain outside point", range.contains("aa"));
    Assert.assertTrue("Should contain lower endpoint", range.contains("ab"));
    Assert.assertFalse("Should not contain upper endpoint", range.contains("cd"));
  }

  @Test
  public void testAtLeastRangeComparable() {
    Range<String> range = Range.atLeast("ab");
    Assert.assertEquals(range.toString(), "[ab, inf)");
    Assert.assertTrue("Should contain inner point", range.contains("b"));
    Assert.assertFalse("Should not contain outside point", range.contains("aa"));
    Assert.assertTrue("Should contain lower endpoint", range.contains("ab"));
    Assert.assertTrue("Should contain max", range.contains("zzzzzzz"));
  }

  @Test
  public void testGreaterThanRangeComparable() {
    Range<String> range = Range.greaterThan("ab");
    Assert.assertEquals(range.toString(), "(ab, inf)");
    Assert.assertTrue("Should contain inner point", range.contains("b"));
    Assert.assertFalse("Should not contain outside point", range.contains("aa"));
    Assert.assertFalse("Should not contain lower endpoint", range.contains("ab"));
    Assert.assertTrue("Should contain max", range.contains("zzzzzzz"));
  }

  @Test
  public void testAtMostRangeComparable() {
    Range<String> range = Range.atMost("cd");
    Assert.assertEquals(range.toString(), "(inf, cd]");
    Assert.assertTrue("Should contain inner point", range.contains("b"));
    Assert.assertFalse("Should not contain outside point", range.contains("d"));
    Assert.assertTrue("Should contain min", range.contains(""));
    Assert.assertTrue("Should contain upper endpoint", range.contains("cd"));
  }

  @Test
  public void testLessThanRangeComparable() {
    Range<String> range = Range.lessThan("cd");
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
        Range.asGuavaRange(Range.singleton("ab")));
    Assert.assertEquals("Should convert to guava open",
        com.google.common.collect.Ranges.open("ab", "cd"),
        Range.asGuavaRange(Range.open("ab", "cd")));
    Assert.assertEquals("Should convert to guava closed",
        com.google.common.collect.Ranges.closed("ab", "cd"),
        Range.asGuavaRange(Range.closed("ab", "cd")));
    Assert.assertEquals("Should convert to guava openClosed",
        com.google.common.collect.Ranges.openClosed("ab", "cd"),
        Range.asGuavaRange(Range.openClosed("ab", "cd")));
    Assert.assertEquals("Should convert to guava closedOpen",
        com.google.common.collect.Ranges.closedOpen("ab", "cd"),
        Range.asGuavaRange(Range.closedOpen("ab", "cd")));
    Assert.assertEquals("Should convert to guava atLeast",
        com.google.common.collect.Ranges.atLeast("ab"),
        Range.asGuavaRange(Range.atLeast("ab")));
    Assert.assertEquals("Should convert to guava greaterThan",
        com.google.common.collect.Ranges.greaterThan("ab"),
        Range.asGuavaRange(Range.greaterThan("ab")));
    Assert.assertEquals("Should convert to guava atMost",
        com.google.common.collect.Ranges.atMost("cd"),
        Range.asGuavaRange(Range.atMost("cd")));
    Assert.assertEquals("Should convert to guava lessThan",
        com.google.common.collect.Ranges.lessThan("cd"),
        Range.asGuavaRange(Range.lessThan("cd")));
  }

  @Test
  public void testIntersectionOverlap() {
    Assert.assertEquals("Open inner endpoints remain open",
        Range.open("d", "dz"),
        Range.closedOpen("cd", "dz").intersection(Range.openClosed("d", "ee")));
    Assert.assertEquals("Open inner endpoints remain open",
        Range.open("d", "dz"),
        Range.openClosed("d", "ee").intersection(Range.closedOpen("cd", "dz")));

    Assert.assertEquals("Closed inner endpoints remain closed",
        Range.closed("d", "dz"),
        Range.openClosed("cd", "dz").intersection(Range.closedOpen("d", "ee")));
    Assert.assertEquals("Closed inner endpoints remain closed",
        Range.closed("d", "dz"),
        Range.closedOpen("d", "ee").intersection(Range.openClosed("cd", "dz")));

    Assert.assertEquals("Start endpoints equal, one open",
        Range.openClosed("cd", "dz"),
        Range.openClosed("cd", "dz").intersection(Range.closedOpen("cd", "ee")));
    Assert.assertEquals("Start endpoints equal, one open",
        Range.openClosed("cd", "dz"),
        Range.closedOpen("cd", "ee").intersection(Range.openClosed("cd", "dz")));

    Assert.assertEquals("Start endpoints equal, both closed",
        Range.closedOpen("cd", "dz"),
        Range.closedOpen("cd", "dz").intersection(Range.closedOpen("cd", "ee")));
    Assert.assertEquals("Start endpoints equal, both closed",
        Range.closedOpen("cd", "dz"),
        Range.closedOpen("cd", "ee").intersection(Range.closedOpen("cd", "dz")));

    Assert.assertEquals("Stop endpoints equal, one open",
        Range.open("d", "ee"),
        Range.openClosed("d", "ee").intersection(Range.closedOpen("cd", "ee")));
    Assert.assertEquals("Stop endpoints equal, one open",
        Range.open("d", "ee"),
        Range.closedOpen("cd", "ee").intersection(Range.openClosed("d", "ee")));

    Assert.assertEquals("Stop endpoints equal, both closed",
        Range.openClosed("d", "ee"),
        Range.openClosed("d", "ee").intersection(Range.openClosed("cd", "ee")));
    Assert.assertEquals("Stop endpoints equal, both closed",
        Range.openClosed("d", "ee"),
        Range.openClosed("cd", "ee").intersection(Range.openClosed("d", "ee")));
  }

  @Test
  public void testIntersectionPositiveInfinity() {
    Assert.assertEquals("Defined endpoint is used",
        Range.closed("d", "ee"),
        Range.openClosed("cd", "ee").intersection(Range.atLeast("d")));
    Assert.assertEquals("Defined endpoint is used",
        Range.closed("d", "ee"),
        Range.atLeast("d").intersection(Range.openClosed("cd", "ee")));
    Assert.assertEquals("Defined endpoint is used",
        Range.open("d", "ee"),
        Range.closedOpen("cd", "ee").intersection(Range.greaterThan("d")));
    Assert.assertEquals("Defined endpoint is used",
        Range.open("d", "ee"),
        Range.greaterThan("d").intersection(Range.closedOpen("cd", "ee")));
  }

  @Test
  public void testIntersectionNegativeInfinity() {
    Assert.assertEquals("Defined endpoint is used",
        Range.openClosed("cd", "d"),
        Range.openClosed("cd", "ee").intersection(Range.atMost("d")));
    Assert.assertEquals("Defined endpoint is used",
        Range.openClosed("cd", "d"),
        Range.atMost("d").intersection(Range.openClosed("cd", "ee")));
    Assert.assertEquals("Defined endpoint is used",
        Range.closedOpen("cd", "d"),
        Range.closedOpen("cd", "ee").intersection(Range.lessThan("d")));
    Assert.assertEquals("Defined endpoint is used",
        Range.closedOpen("cd", "d"),
        Range.lessThan("d").intersection(Range.closedOpen("cd", "ee")));
  }
}
