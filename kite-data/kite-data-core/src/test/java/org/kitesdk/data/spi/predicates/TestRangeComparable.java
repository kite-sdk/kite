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

package org.kitesdk.data.spi.predicates;

import org.junit.Assert;
import org.junit.Test;

public class TestRangeComparable {
  @Test
  public void testSingletonRangeComparable() {
    Range<Integer> range = Ranges.singleton(3);
    Assert.assertEquals(range.toString(), "[3, 3]");
    Assert.assertTrue("Should contain inner point", range.contains(3));
    Assert.assertFalse("Should not contain outside point", range.contains(2));
  }

  @Test
  public void testOpenRangeComparable() {
    Range<Integer> range = Ranges.open(3, 5);
    Assert.assertEquals(range.toString(), "(3, 5)");
    Assert.assertTrue("Should contain inner point", range.contains(4));
    Assert.assertFalse("Should not contain outside point", range.contains(2));
    Assert.assertFalse("Should not contain lower endpoint", range.contains(3));
    Assert.assertFalse("Should not contain upper endpoint", range.contains(5));
  }

  @Test
  public void testClosedRangeComparable() {
    Range<Integer> range = Ranges.closed(3, 5);
    Assert.assertEquals(range.toString(), "[3, 5]");
    Assert.assertTrue("Should contain inner point", range.contains(4));
    Assert.assertFalse("Should not contain outside point", range.contains(2));
    Assert.assertTrue("Should contain lower endpoint", range.contains(3));
    Assert.assertTrue("Should contain upper endpoint", range.contains(5));
  }

  @Test
  public void testOpenClosedRangeComparable() {
    Range<Integer> range = Ranges.openClosed(3, 5);
    Assert.assertEquals(range.toString(), "(3, 5]");
    Assert.assertTrue("Should contain inner point", range.contains(4));
    Assert.assertFalse("Should not contain outside point", range.contains(2));
    Assert.assertFalse("Should not contain lower endpoint", range.contains(3));
    Assert.assertTrue("Should contain upper endpoint", range.contains(5));
  }

  @Test
  public void testClosedOpenRangeComparable() {
    Range<Integer> range = Ranges.closedOpen(3, 5);
    Assert.assertEquals(range.toString(), "[3, 5)");
    Assert.assertTrue("Should contain inner point", range.contains(4));
    Assert.assertFalse("Should not contain outside point", range.contains(2));
    Assert.assertTrue("Should contain lower endpoint", range.contains(3));
    Assert.assertFalse("Should not contain upper endpoint", range.contains(5));
  }

  @Test
  public void testAtLeastRangeComparable() {
    Range<Integer> range = Ranges.atLeast(3);
    Assert.assertEquals(range.toString(), "[3, inf)");
    Assert.assertTrue("Should contain inner point", range.contains(4));
    Assert.assertFalse("Should not contain outside point", range.contains(2));
    Assert.assertTrue("Should contain lower endpoint", range.contains(3));
    Assert.assertTrue("Should contain max", range.contains(Integer.MAX_VALUE));
  }

  @Test
  public void testGreaterThanRangeComparable() {
    Range<Integer> range = Ranges.greaterThan(3);
    Assert.assertEquals(range.toString(), "(3, inf)");
    Assert.assertTrue("Should contain inner point", range.contains(4));
    Assert.assertFalse("Should not contain outside point", range.contains(2));
    Assert.assertFalse("Should not contain lower endpoint", range.contains(3));
    Assert.assertTrue("Should contain max", range.contains(Integer.MAX_VALUE));
  }

  @Test
  public void testAtMostRangeComparable() {
    Range<Integer> range = Ranges.atMost(5);
    Assert.assertEquals(range.toString(), "(inf, 5]");
    Assert.assertTrue("Should contain inner point", range.contains(4));
    Assert.assertFalse("Should not contain outside point", range.contains(6));
    Assert.assertTrue("Should contain min", range.contains(Integer.MIN_VALUE));
    Assert.assertTrue("Should contain upper endpoint", range.contains(5));
  }

  @Test
  public void testLessThanRangeComparable() {
    Range<Integer> range = Ranges.lessThan(5);
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
        Ranges.asGuavaRange(Ranges.singleton(3)));
    Assert.assertEquals("Should convert to guava open",
        com.google.common.collect.Ranges.open(3, 5),
        Ranges.asGuavaRange(Ranges.open(3, 5)));
    Assert.assertEquals("Should convert to guava closed",
        com.google.common.collect.Ranges.closed(3, 5),
        Ranges.asGuavaRange(Ranges.closed(3, 5)));
    Assert.assertEquals("Should convert to guava openClosed",
        com.google.common.collect.Ranges.openClosed(3, 5),
        Ranges.asGuavaRange(Ranges.openClosed(3, 5)));
    Assert.assertEquals("Should convert to guava closedOpen",
        com.google.common.collect.Ranges.closedOpen(3, 5),
        Ranges.asGuavaRange(Ranges.closedOpen(3, 5)));
    Assert.assertEquals("Should convert to guava atLeast",
        com.google.common.collect.Ranges.atLeast(3),
        Ranges.asGuavaRange(Ranges.atLeast(3)));
    Assert.assertEquals("Should convert to guava greaterThan",
        com.google.common.collect.Ranges.greaterThan(3),
        Ranges.asGuavaRange(Ranges.greaterThan(3)));
    Assert.assertEquals("Should convert to guava atMost",
        com.google.common.collect.Ranges.atMost(5),
        Ranges.asGuavaRange(Ranges.atMost(5)));
    Assert.assertEquals("Should convert to guava lessThan",
        com.google.common.collect.Ranges.lessThan(5),
        Ranges.asGuavaRange(Ranges.lessThan(5)));
  }

  @Test
  public void testIntersectionOverlap() {
    Assert.assertEquals("Open inner endpoints remain open",
        Ranges.open(10, 15),
        Ranges.closedOpen(5, 15).intersection(Ranges.openClosed(10, 20)));
    Assert.assertEquals("Open inner endpoints remain open",
        Ranges.open(10, 15),
        Ranges.openClosed(10, 20).intersection(Ranges.closedOpen(5, 15)));

    Assert.assertEquals("Closed inner endpoints remain closed",
        Ranges.closed(10, 15),
        Ranges.openClosed(5, 15).intersection(Ranges.closedOpen(10, 20)));
    Assert.assertEquals("Closed inner endpoints remain closed",
        Ranges.closed(10, 15),
        Ranges.closedOpen(10, 20).intersection(Ranges.openClosed(5, 15)));

    Assert.assertEquals("Start endpoints equal, one open",
        Ranges.openClosed(5, 15),
        Ranges.openClosed(5, 15).intersection(Ranges.closedOpen(5, 20)));
    Assert.assertEquals("Start endpoints equal, one open",
        Ranges.openClosed(5, 15),
        Ranges.closedOpen(5, 20).intersection(Ranges.openClosed(5, 15)));

    Assert.assertEquals("Start endpoints equal, both closed",
        Ranges.closedOpen(5, 15),
        Ranges.closedOpen(5, 15).intersection(Ranges.closedOpen(5, 20)));
    Assert.assertEquals("Start endpoints equal, both closed",
        Ranges.closedOpen(5, 15),
        Ranges.closedOpen(5, 20).intersection(Ranges.closedOpen(5, 15)));

    Assert.assertEquals("Stop endpoints equal, one open",
        Ranges.open(10, 20),
        Ranges.openClosed(10, 20).intersection(Ranges.closedOpen(5, 20)));
    Assert.assertEquals("Stop endpoints equal, one open",
        Ranges.open(10, 20),
        Ranges.closedOpen(5, 20).intersection(Ranges.openClosed(10, 20)));

    Assert.assertEquals("Stop endpoints equal, both closed",
        Ranges.openClosed(10, 20),
        Ranges.openClosed(10, 20).intersection(Ranges.openClosed(5, 20)));
    Assert.assertEquals("Stop endpoints equal, both closed",
        Ranges.openClosed(10, 20),
        Ranges.openClosed(5, 20).intersection(Ranges.openClosed(10, 20)));
  }

  @Test
  public void testIntersectionPositiveInfinity() {
    Assert.assertEquals("Defined endpoint is used",
        Ranges.closed(10, 20),
        Ranges.openClosed(5, 20).intersection(Ranges.atLeast(10)));
    Assert.assertEquals("Defined endpoint is used",
        Ranges.closed(10, 20),
        Ranges.atLeast(10).intersection(Ranges.openClosed(5, 20)));
    Assert.assertEquals("Defined endpoint is used",
        Ranges.open(10, 20),
        Ranges.closedOpen(5, 20).intersection(Ranges.greaterThan(10)));
    Assert.assertEquals("Defined endpoint is used",
        Ranges.open(10, 20),
        Ranges.greaterThan(10).intersection(Ranges.closedOpen(5, 20)));
  }

  @Test
  public void testIntersectionNegativeInfinity() {
    Assert.assertEquals("Defined endpoint is used",
        Ranges.openClosed(5, 10),
        Ranges.openClosed(5, 20).intersection(Ranges.atMost(10)));
    Assert.assertEquals("Defined endpoint is used",
        Ranges.openClosed(5, 10),
        Ranges.atMost(10).intersection(Ranges.openClosed(5, 20)));
    Assert.assertEquals("Defined endpoint is used",
        Ranges.closedOpen(5, 10),
        Ranges.closedOpen(5, 20).intersection(Ranges.lessThan(10)));
    Assert.assertEquals("Defined endpoint is used",
        Ranges.closedOpen(5, 10),
        Ranges.lessThan(10).intersection(Ranges.closedOpen(5, 20)));
  }
}
