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

import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.TestHelpers;

import static org.junit.Assert.assertEquals;

public class TestMarkerRange {

  private static Marker OCT_12;
  private static Marker OCT_15;
  private static Marker SEPT_30;
  private static Marker NOV_1;
  private static MarkerComparator comparator;

  @BeforeClass
  public static void setup() {
    OCT_12 = new Marker.Builder()
        .add("year", 2013)
        .add("month", 10)
        .add("day", 12)
        .build();
    OCT_15 = new Marker.Builder()
        .add("year", 2013)
        .add("month", 10)
        .add("day", 15)
        .build();
    SEPT_30 = new Marker.Builder()
        .add("year", 2013)
        .add("month", 9)
        .add("day", 30)
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

  @Test
  public void testIntersection() {
    final MarkerRange unbounded = new MarkerRange(comparator);
    final MarkerRange unboundedToNov1 = new MarkerRange(comparator).to(NOV_1);
    final MarkerRange oct12ToOct15 = new MarkerRange(comparator).from(OCT_12).to(OCT_15);
    final MarkerRange sept30ToOct15 = new MarkerRange(comparator).from(SEPT_30).to(OCT_15);
    final MarkerRange oct12ToNov1 = new MarkerRange(comparator).from(OCT_12).to(NOV_1);
    final MarkerRange sept30ToNov1 = new MarkerRange(comparator).from(SEPT_30).to(NOV_1);
    final MarkerRange sept30ToUnbounded = new MarkerRange(comparator).from(SEPT_30);
    final MarkerRange nov1ToUnbounded = new MarkerRange(comparator).from(NOV_1);

    // Intersection with self
    assertEquals(unbounded, unbounded.intersection(unbounded));
    assertEquals(oct12ToOct15, oct12ToOct15.intersection(oct12ToOct15));
    assertEquals(unboundedToNov1, unboundedToNov1.intersection(unboundedToNov1));
    assertEquals(sept30ToUnbounded, sept30ToUnbounded.intersection(sept30ToUnbounded));

    // Intersection with double unbounded
    assertEquals(oct12ToOct15, unbounded.intersection(oct12ToOct15));
    assertEquals(oct12ToOct15, oct12ToOct15.intersection(unbounded));

    // Intersection with single unbounded
    assertEquals(oct12ToOct15, unboundedToNov1.intersection(oct12ToOct15));
    assertEquals(oct12ToOct15, sept30ToUnbounded.intersection(oct12ToOct15));
    assertEquals(nov1ToUnbounded, nov1ToUnbounded.intersection(sept30ToUnbounded));
    assertEquals(nov1ToUnbounded, sept30ToUnbounded.intersection(nov1ToUnbounded));

    // Fully-contained
    assertEquals(oct12ToOct15, sept30ToNov1.intersection(oct12ToOct15));
    assertEquals(oct12ToOct15, oct12ToOct15.intersection(sept30ToNov1));

    // Overlap
    assertEquals(oct12ToOct15, sept30ToOct15.intersection(oct12ToNov1));
    assertEquals(oct12ToOct15, oct12ToNov1.intersection(sept30ToOct15));

    // No overlap
    TestHelpers.assertThrows("Can't find intersection when no overlap.",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        sept30ToOct15.intersection(nov1ToUnbounded);
      }
    });
    TestHelpers.assertThrows("Can't find intersection when no overlap.",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        nov1ToUnbounded.intersection(sept30ToOct15);
      }
    });

  }

  @Test
  public void testSpan() {
    final MarkerRange unbounded = new MarkerRange(comparator);
    final MarkerRange unboundedToNov1 = new MarkerRange(comparator).to(NOV_1);
    final MarkerRange oct12ToOct15 = new MarkerRange(comparator).from(OCT_12).to(OCT_15);
    final MarkerRange sept30ToOct15 = new MarkerRange(comparator).from(SEPT_30).to(OCT_15);
    final MarkerRange oct12ToNov1 = new MarkerRange(comparator).from(OCT_12).to(NOV_1);
    final MarkerRange sept30ToNov1 = new MarkerRange(comparator).from(SEPT_30).to(NOV_1);
    final MarkerRange sept30ToUnbounded = new MarkerRange(comparator).from(SEPT_30);
    final MarkerRange nov1ToUnbounded = new MarkerRange(comparator).from(NOV_1);

    // Span with self
    assertEquals(unbounded, unbounded.span(unbounded));
    assertEquals(oct12ToOct15, oct12ToOct15.span(oct12ToOct15));
    assertEquals(unboundedToNov1, unboundedToNov1.span(unboundedToNov1));
    assertEquals(sept30ToUnbounded, sept30ToUnbounded.span(sept30ToUnbounded));

    // Intersection with double unbounded
    assertEquals(unbounded, unbounded.span(oct12ToOct15));
    assertEquals(unbounded, oct12ToOct15.span(unbounded));

    // Intersection with single unbounded
    assertEquals(unboundedToNov1, unboundedToNov1.span(oct12ToOct15));
    assertEquals(sept30ToUnbounded, sept30ToUnbounded.span(oct12ToOct15));
    assertEquals(sept30ToUnbounded, nov1ToUnbounded.span(sept30ToUnbounded));
    assertEquals(sept30ToUnbounded, sept30ToUnbounded.span(nov1ToUnbounded));

    // Fully-contained
    assertEquals(sept30ToNov1, sept30ToNov1.span(oct12ToOct15));
    assertEquals(sept30ToNov1, oct12ToOct15.span(sept30ToNov1));

    // Overlap
    assertEquals(sept30ToNov1, sept30ToOct15.span(oct12ToNov1));
    assertEquals(sept30ToNov1, oct12ToNov1.span(sept30ToOct15));

    // No overlap
    assertEquals(sept30ToUnbounded, sept30ToOct15.span(nov1ToUnbounded));
    assertEquals(sept30ToUnbounded, nov1ToUnbounded.span(sept30ToOct15));
  }

  @Test
  public void testComplement() {
    final MarkerRange unbounded = new MarkerRange(comparator);
    final MarkerRange unboundedToNov1 = new MarkerRange(comparator).to(NOV_1);
    final MarkerRange oct12ToOct15 = new MarkerRange(comparator).from(OCT_12).to(OCT_15);
    final MarkerRange afterNov1ToUnbounded = new MarkerRange(comparator).fromAfter(NOV_1);

    TestHelpers.assertThrows("Can't find complement of unbounded range.",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        unbounded.complement();
      }
    });

    assertEquals(afterNov1ToUnbounded, unboundedToNov1.complement());
    assertEquals(unboundedToNov1, afterNov1ToUnbounded.complement());
    assertEquals(unbounded, oct12ToOct15.complement());

  }
}
