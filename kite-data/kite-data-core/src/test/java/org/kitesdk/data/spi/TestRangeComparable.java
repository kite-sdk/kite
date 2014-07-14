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

import org.junit.Assert;
import org.junit.Test;

public class TestRangeComparable {
  @Test
  public void testSingletonRangeComparable() {
    Range<Integer> range = Range.singleton(3);
    Assert.assertEquals(range.toString(), "[3, 3]");
    Assert.assertTrue("Should contain inner point", range.contains(3));
    Assert.assertFalse("Should not contain outside point", range.contains(2));
  }

  @Test
  public void testOpenRangeComparable() {
    Range<Integer> range = Range.open(3, 5);
    Assert.assertEquals(range.toString(), "(3, 5)");
    Assert.assertTrue("Should contain inner point", range.contains(4));
    Assert.assertFalse("Should not contain outside point", range.contains(2));
    Assert.assertFalse("Should not contain lower endpoint", range.contains(3));
    Assert.assertFalse("Should not contain upper endpoint", range.contains(5));
  }

  @Test
  public void testClosedRangeComparable() {
    Range<Integer> range = Range.closed(3, 5);
    Assert.assertEquals(range.toString(), "[3, 5]");
    Assert.assertTrue("Should contain inner point", range.contains(4));
    Assert.assertFalse("Should not contain outside point", range.contains(2));
    Assert.assertTrue("Should contain lower endpoint", range.contains(3));
    Assert.assertTrue("Should contain upper endpoint", range.contains(5));
  }

  @Test
  public void testOpenClosedRangeComparable() {
    Range<Integer> range = Range.openClosed(3, 5);
    Assert.assertEquals(range.toString(), "(3, 5]");
    Assert.assertTrue("Should contain inner point", range.contains(4));
    Assert.assertFalse("Should not contain outside point", range.contains(2));
    Assert.assertFalse("Should not contain lower endpoint", range.contains(3));
    Assert.assertTrue("Should contain upper endpoint", range.contains(5));
  }

  @Test
  public void testClosedOpenRangeComparable() {
    Range<Integer> range = Range.closedOpen(3, 5);
    Assert.assertEquals(range.toString(), "[3, 5)");
    Assert.assertTrue("Should contain inner point", range.contains(4));
    Assert.assertFalse("Should not contain outside point", range.contains(2));
    Assert.assertTrue("Should contain lower endpoint", range.contains(3));
    Assert.assertFalse("Should not contain upper endpoint", range.contains(5));
  }

  @Test
  public void testAtLeastRangeComparable() {
    Range<Integer> range = Range.atLeast(3);
    Assert.assertEquals(range.toString(), "[3, inf)");
    Assert.assertTrue("Should contain inner point", range.contains(4));
    Assert.assertFalse("Should not contain outside point", range.contains(2));
    Assert.assertTrue("Should contain lower endpoint", range.contains(3));
    Assert.assertTrue("Should contain max", range.contains(Integer.MAX_VALUE));
  }

  @Test
  public void testGreaterThanRangeComparable() {
    Range<Integer> range = Range.greaterThan(3);
    Assert.assertEquals(range.toString(), "(3, inf)");
    Assert.assertTrue("Should contain inner point", range.contains(4));
    Assert.assertFalse("Should not contain outside point", range.contains(2));
    Assert.assertFalse("Should not contain lower endpoint", range.contains(3));
    Assert.assertTrue("Should contain max", range.contains(Integer.MAX_VALUE));
  }

  @Test
  public void testAtMostRangeComparable() {
    Range<Integer> range = Range.atMost(5);
    Assert.assertEquals(range.toString(), "(inf, 5]");
    Assert.assertTrue("Should contain inner point", range.contains(4));
    Assert.assertFalse("Should not contain outside point", range.contains(6));
    Assert.assertTrue("Should contain min", range.contains(Integer.MIN_VALUE));
    Assert.assertTrue("Should contain upper endpoint", range.contains(5));
  }

  @Test
  public void testLessThanRangeComparable() {
    Range<Integer> range = Range.lessThan(5);
    Assert.assertEquals(range.toString(), "(inf, 5)");
    Assert.assertTrue("Should contain inner point", range.contains(4));
    Assert.assertFalse("Should not contain outside point", range.contains(6));
    Assert.assertTrue("Should contain min", range.contains(Integer.MIN_VALUE));
    Assert.assertFalse("Should not contain upper endpoint", range.contains(5));
  }

  @Test
  public void testAsGuavaRange() {
    // some methods like asSet convert to Guava rather than reimplementing
    Assert.assertEquals("Should convert to guava singleton",
        com.google.common.collect.Ranges.singleton(3),
        Range.asGuavaRange(Range.singleton(3)));
    Assert.assertEquals("Should convert to guava open",
        com.google.common.collect.Ranges.open(3, 5),
        Range.asGuavaRange(Range.open(3, 5)));
    Assert.assertEquals("Should convert to guava closed",
        com.google.common.collect.Ranges.closed(3, 5),
        Range.asGuavaRange(Range.closed(3, 5)));
    Assert.assertEquals("Should convert to guava openClosed",
        com.google.common.collect.Ranges.openClosed(3, 5),
        Range.asGuavaRange(Range.openClosed(3, 5)));
    Assert.assertEquals("Should convert to guava closedOpen",
        com.google.common.collect.Ranges.closedOpen(3, 5),
        Range.asGuavaRange(Range.closedOpen(3, 5)));
    Assert.assertEquals("Should convert to guava atLeast",
        com.google.common.collect.Ranges.atLeast(3),
        Range.asGuavaRange(Range.atLeast(3)));
    Assert.assertEquals("Should convert to guava greaterThan",
        com.google.common.collect.Ranges.greaterThan(3),
        Range.asGuavaRange(Range.greaterThan(3)));
    Assert.assertEquals("Should convert to guava atMost",
        com.google.common.collect.Ranges.atMost(5),
        Range.asGuavaRange(Range.atMost(5)));
    Assert.assertEquals("Should convert to guava lessThan",
        com.google.common.collect.Ranges.lessThan(5),
        Range.asGuavaRange(Range.lessThan(5)));
  }

  @Test
  public void testIntersectionOverlap() {
    Assert.assertEquals("Open inner endpoints remain open",
        Range.open(10, 15),
        Range.closedOpen(5, 15).intersection(Range.openClosed(10, 20)));
    Assert.assertEquals("Open inner endpoints remain open",
        Range.open(10, 15),
        Range.openClosed(10, 20).intersection(Range.closedOpen(5, 15)));

    Assert.assertEquals("Closed inner endpoints remain closed",
        Range.closed(10, 15),
        Range.openClosed(5, 15).intersection(Range.closedOpen(10, 20)));
    Assert.assertEquals("Closed inner endpoints remain closed",
        Range.closed(10, 15),
        Range.closedOpen(10, 20).intersection(Range.openClosed(5, 15)));

    Assert.assertEquals("Start endpoints equal, one open",
        Range.openClosed(5, 15),
        Range.openClosed(5, 15).intersection(Range.closedOpen(5, 20)));
    Assert.assertEquals("Start endpoints equal, one open",
        Range.openClosed(5, 15),
        Range.closedOpen(5, 20).intersection(Range.openClosed(5, 15)));

    Assert.assertEquals("Start endpoints equal, both closed",
        Range.closedOpen(5, 15),
        Range.closedOpen(5, 15).intersection(Range.closedOpen(5, 20)));
    Assert.assertEquals("Start endpoints equal, both closed",
        Range.closedOpen(5, 15),
        Range.closedOpen(5, 20).intersection(Range.closedOpen(5, 15)));

    Assert.assertEquals("Stop endpoints equal, one open",
        Range.open(10, 20),
        Range.openClosed(10, 20).intersection(Range.closedOpen(5, 20)));
    Assert.assertEquals("Stop endpoints equal, one open",
        Range.open(10, 20),
        Range.closedOpen(5, 20).intersection(Range.openClosed(10, 20)));

    Assert.assertEquals("Stop endpoints equal, both closed",
        Range.openClosed(10, 20),
        Range.openClosed(10, 20).intersection(Range.openClosed(5, 20)));
    Assert.assertEquals("Stop endpoints equal, both closed",
        Range.openClosed(10, 20),
        Range.openClosed(5, 20).intersection(Range.openClosed(10, 20)));
  }

  @Test
  public void testIntersectionPositiveInfinity() {
    Assert.assertEquals("Defined endpoint is used",
        Range.closed(10, 20),
        Range.openClosed(5, 20).intersection(Range.atLeast(10)));
    Assert.assertEquals("Defined endpoint is used",
        Range.closed(10, 20),
        Range.atLeast(10).intersection(Range.openClosed(5, 20)));
    Assert.assertEquals("Defined endpoint is used",
        Range.open(10, 20),
        Range.closedOpen(5, 20).intersection(Range.greaterThan(10)));
    Assert.assertEquals("Defined endpoint is used",
        Range.open(10, 20),
        Range.greaterThan(10).intersection(Range.closedOpen(5, 20)));
  }

  @Test
  public void testIntersectionNegativeInfinity() {
    Assert.assertEquals("Defined endpoint is used",
        Range.openClosed(5, 10),
        Range.openClosed(5, 20).intersection(Range.atMost(10)));
    Assert.assertEquals("Defined endpoint is used",
        Range.openClosed(5, 10),
        Range.atMost(10).intersection(Range.openClosed(5, 20)));
    Assert.assertEquals("Defined endpoint is used",
        Range.closedOpen(5, 10),
        Range.closedOpen(5, 20).intersection(Range.lessThan(10)));
    Assert.assertEquals("Defined endpoint is used",
        Range.closedOpen(5, 10),
        Range.lessThan(10).intersection(Range.closedOpen(5, 20)));
  }
}
