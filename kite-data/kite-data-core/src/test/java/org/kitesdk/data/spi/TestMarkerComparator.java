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

package org.kitesdk.data.spi;

import org.kitesdk.data.PartitionStrategy;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMarkerComparator {

  // Left-inclusive comparison is implemented by leftCompare
  // this is used when you want to use the comparison for the lower-bound of a range.

  private static Marker OCT;
  private static Marker OCT_12;
  private static Marker SEPT;
  private static Marker SEPT_30;
  private static Marker NOV;
  private static Marker NOV_1;
  private static MarkerComparator comparator;

  @BeforeClass
  public static void setup() {
    OCT = new Marker.Builder()
        .add("year", 2013)
        .add("month", 10)
        .build();
    OCT_12 = new Marker.Builder()
        .add("year", 2013)
        .add("month", 10)
        .add("day", 12)
        .build();
    SEPT = new Marker.Builder()
        .add("year", 2013)
        .add("month", 9)
        .build();
    SEPT_30 = new Marker.Builder()
        .add("year", 2013)
        .add("month", 9)
        .add("day", 30)
        .build();
    NOV = new Marker.Builder()
        .add("year", 2013)
        .add("month", 11)
        .build();
    NOV_1 = new Marker.Builder()
        .add("year", 2013)
        .add("month", 11)
        .add("day", 1)
        .build();
    PartitionStrategy strategy = new PartitionStrategy.Builder()
        .year("timestamp")
        .month("timestamp")
        .day("timestamp")
        .build();
    comparator = new MarkerComparator(strategy);
  }

  @Test(expected=IllegalStateException.class)
  public void testContainedStateException() {
    comparator.compare(OCT, OCT_12);
  }

  @Test
  public void testLeftInclusiveContained() {
    // left-inclusive comparison is for range lower bounds, so contained
    // should be greater than and therefore included above the lower bound.
    Assert.assertEquals(-1, comparator.leftCompare(OCT, OCT_12));
    Assert.assertEquals(1, comparator.leftCompare(OCT_12, OCT));
  }

  @Test
  public void testRightInclusiveContained() {
    // right-inclusive comparison is for range upper bounds, so contained
    // should be less than and therefore included below the upper bound.
    Assert.assertEquals(1, comparator.rightCompare(OCT, OCT_12));
    Assert.assertEquals(-1, comparator.rightCompare(OCT_12, OCT));
  }

  @Test
  public void testLessThan() {
    // lessThan should always be less
    Assert.assertEquals(-1, comparator.compare(SEPT_30, OCT));
    Assert.assertEquals(-1, comparator.leftCompare(SEPT_30, OCT));
    Assert.assertEquals(-1, comparator.rightCompare(SEPT_30, OCT));
    Assert.assertEquals(1, comparator.compare(OCT, SEPT_30));
    Assert.assertEquals(1, comparator.leftCompare(OCT, SEPT_30));
    Assert.assertEquals(1, comparator.rightCompare(OCT, SEPT_30));
  }

  @Test
  public void testGreaterThan() {
    // greaterThan should always be greater
    Assert.assertEquals(1, comparator.compare(NOV_1, OCT));
    Assert.assertEquals(1, comparator.leftCompare(NOV_1, OCT));
    Assert.assertEquals(1, comparator.rightCompare(NOV_1, OCT));
    Assert.assertEquals(-1, comparator.compare(OCT, NOV_1));
    Assert.assertEquals(-1, comparator.leftCompare(OCT, NOV_1));
    Assert.assertEquals(-1, comparator.rightCompare(OCT, NOV_1));
  }

  @Test
  public void testEquals() {
    Assert.assertEquals(0, comparator.compare(OCT, OCT));
    Assert.assertEquals(0, comparator.compare(SEPT_30, SEPT_30));
    Assert.assertEquals(0, comparator.compare(OCT_12, OCT_12));
    Assert.assertEquals(0, comparator.compare(NOV_1, NOV_1));
    Assert.assertEquals(0, comparator.leftCompare(OCT, OCT));
    Assert.assertEquals(0, comparator.leftCompare(SEPT_30, SEPT_30));
    Assert.assertEquals(0, comparator.leftCompare(OCT_12, OCT_12));
    Assert.assertEquals(0, comparator.leftCompare(NOV_1, NOV_1));
    Assert.assertEquals(0, comparator.rightCompare(OCT, OCT));
    Assert.assertEquals(0, comparator.rightCompare(SEPT_30, SEPT_30));
    Assert.assertEquals(0, comparator.rightCompare(OCT_12, OCT_12));
    Assert.assertEquals(0, comparator.rightCompare(NOV_1, NOV_1));
  }

  @Test
  public void testContains() {
    Assert.assertTrue(comparator.contains(OCT, OCT_12));
    Assert.assertTrue(comparator.contains(OCT, OCT));
    Assert.assertFalse(comparator.contains(OCT_12, OCT));
    Assert.assertFalse(comparator.contains(OCT, SEPT));
    Assert.assertFalse(comparator.contains(OCT, SEPT_30));
    Assert.assertFalse(comparator.contains(OCT, NOV));
    Assert.assertFalse(comparator.contains(OCT, NOV_1));
  }
}
