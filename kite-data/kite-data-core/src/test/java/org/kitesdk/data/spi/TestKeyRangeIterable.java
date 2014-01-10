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

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.util.Set;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.data.FieldPartitioner;
import org.kitesdk.data.PartitionStrategy;

public class TestKeyRangeIterable {
  public static final PartitionStrategy id = new PartitionStrategy.Builder()
      .identity("component", String.class, 50)
      .build();
  public static final MarkerComparator idCmp = new MarkerComparator(id);

  public static final PartitionStrategy hash = new PartitionStrategy.Builder()
      .hash("id", "id-hash", 64)
      .identity("id", String.class, 50)
      .build();
  public static final MarkerComparator hashCmp = new MarkerComparator(hash);

  public static final PartitionStrategy num = new PartitionStrategy.Builder()
      .identity("number", Integer.class, 50)
      .build();
  public static final MarkerComparator numCmp = new MarkerComparator(num);

  public static final PartitionStrategy num2 = new PartitionStrategy.Builder()
      .identity("number", Integer.class, 50)
      .identity("number2", Integer.class, 50)
      .build();
  public static final MarkerComparator num2Cmp = new MarkerComparator(num2);

  public static final PartitionStrategy time = new PartitionStrategy.Builder()
      .year("timestamp").month("timestamp").day("timestamp").build();
  public static final MarkerComparator timeCmp = new MarkerComparator(time);

  public static final PartitionStrategy strategy = new PartitionStrategy.Builder()
      .hash("id", "id-hash", 64)
      .year("timestamp").month("timestamp").day("timestamp")
      .identity("component", String.class, 50)
      .identity("id", String.class, 50)
      .build();
  public static final MarkerComparator cmp = new MarkerComparator(strategy);

  @Test
  public void testUnbounded() {
    Constraints c = new Constraints();
    assertIterableEquals(
        Sets.newHashSet(new MarkerRange(idCmp)),
        c.toKeyRanges(id));
  }

  @Test
  public void testSingleSet() {
    Constraints c = new Constraints().with("component", "com.company.Main");

    Marker main = new Marker.Builder("component", "com.company.Main").build();
    MarkerRange actual = Iterables.getOnlyElement(c.toKeyRanges(id));
    Assert.assertEquals(main, actual.getStart().getBound());
    Assert.assertEquals(main, actual.getEnd().getBound());
    Assert.assertEquals(new MarkerRange(idCmp).of(main), actual);

    c = new Constraints().with("component",
        "com.company.Main", "com.company.SomeClass");
    Marker sc = new Marker.Builder("component", "com.company.SomeClass").build();
    assertIterableEquals(
        Sets.newHashSet(
            new MarkerRange(idCmp).of(main),
            new MarkerRange(idCmp).of(sc)),
        c.toKeyRanges(id));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGroupSet() {
    String[] ids = new String[]{
      UUID.randomUUID().toString(), UUID.randomUUID().toString()};
    FieldPartitioner hashFunc = hash.getFieldPartitioners().get(0);

    Constraints c = new Constraints().with("id", ids[0]);

    Marker marker0 = new Marker.Builder()
        .add("id-hash", hashFunc.apply(ids[0])).add("id", ids[0]).build();
    MarkerRange actual = Iterables.getOnlyElement(c.toKeyRanges(hash));
    Assert.assertEquals(marker0, actual.getStart().getBound());
    Assert.assertEquals(marker0, actual.getEnd().getBound());

    c = new Constraints().with("id", (Object[]) ids);
    Marker marker1 = new Marker.Builder()
        .add("id-hash", hashFunc.apply(ids[1])).add("id", ids[1]).build();
    assertIterableEquals(
        Sets.newHashSet(
            new MarkerRange(hashCmp).of(marker0),
            new MarkerRange(hashCmp).of(marker1)),
        c.toKeyRanges(hash));
  }

  @Test
  public void testSingleRange() {
    Constraints c = new Constraints()
        .from("number", 5).toBefore("number", 18)
        .to("number2", 9);
    Marker start = new Marker.Builder("number", 5).build();
    Marker stop = new Marker.Builder().add("number", 18).add("number2", 9).build();

    Assert.assertEquals(
        new MarkerRange(num2Cmp).from(start).to(stop),
        Iterables.getOnlyElement(c.toKeyRanges(num2)));
  }

  @Test
  public void testHashRange() {
    Constraints c = new Constraints().from("id", "0000").toBefore("id", "0001");
    // note the lack of a hash field -- ranges cannot be projected through hash
    Marker start = new Marker.Builder("id", "0000").build();
    Marker stop = new Marker.Builder("id", "0001").build();

    Assert.assertEquals(
        new MarkerRange(hashCmp).from(start).to(stop),
        Iterables.getOnlyElement(c.toKeyRanges(hash)));
  }

  @Test
  public void testGroupRange() {
    Constraints c = new Constraints().from("number", 5).toBefore("number", 18);
    Marker start = new Marker.Builder("number", 5).build();
    Marker stop = new Marker.Builder("number", 18).build();

    Assert.assertEquals(
        new MarkerRange(numCmp).from(start).to(stop),
        Iterables.getOnlyElement(c.toKeyRanges(num)));
  }

  @Test
  public void testTimeSet() {
    Long[] timestamps = new Long[] {
        1379020547042L, // Thu Sep 12 14:15:47 PDT 2013
        1381612547042L, // Sat Oct 12 14:15:47 PDT 2013
        1384204547042L  // Mon Nov 11 13:15:47 PST 2013
    };

    Constraints c = new Constraints().with("timestamp", (Object[]) timestamps);

    Marker sep = new Marker.Builder().add("year", 2013).add("month", 9).add("day", 12).build();
    Marker oct = new Marker.Builder().add("year", 2013).add("month", 10).add("day", 12).build();
    Marker nov = new Marker.Builder().add("year", 2013).add("month", 11).add("day", 11).build();

    assertIterableEquals(Sets.newHashSet(
        new MarkerRange(timeCmp).of(sep),
        new MarkerRange(timeCmp).of(oct),
        new MarkerRange(timeCmp).of(nov)),
        c.toKeyRanges(time));
  }

  @Test
  public void testTimeRange() {
    Long[] timestamps = new Long[] {
        1379020547042L, // Thu Sep 12 14:15:47 PDT 2013
        1381612547042L, // Sat Oct 12 14:15:47 PDT 2013
        1384204547042L  // Mon Nov 11 13:15:47 PST 2013
    };

    Constraints c = new Constraints()
        .from("timestamp", timestamps[0]).to("timestamp", timestamps[2]);

    Marker sep = new Marker.Builder().add("year", 2013).add("month", 9).add("day", 12).build();
    Marker nov = new Marker.Builder().add("year", 2013).add("month", 11).add("day", 11).build();

    Assert.assertEquals(
        new MarkerRange(timeCmp).from(sep).to(nov),
        Iterables.getOnlyElement(c.toKeyRanges(time)));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCombined() {
    Long[] timestamps = new Long[] {
        1379020547042L, // Thu Sep 12 14:15:47 PDT 2013
        1384204547042L  // Mon Nov 11 13:15:47 PST 2013
    };
    String[] ids = new String[]{
        UUID.randomUUID().toString(), UUID.randomUUID().toString()};
    FieldPartitioner hashFunc = hash.getFieldPartitioners().get(0);

    Constraints c = new Constraints()
        .from("timestamp", timestamps[0]).to("timestamp", timestamps[1])
        .with("id", (Object[]) ids);

    // first range
    Marker sep0 = new Marker.Builder()
        .add("year", 2013).add("month", 9).add("day", 12)
        .add("id-hash", hashFunc.apply(ids[0])).add("id", ids[0])
        .build();
    Marker nov0 = new Marker.Builder()
        .add("year", 2013).add("month", 11).add("day", 11)
        .add("id-hash", hashFunc.apply(ids[0])).add("id", ids[0])
        .build();
    // second range
    Marker sep1 = new Marker.Builder()
        .add("year", 2013).add("month", 9).add("day", 12)
        .add("id-hash", hashFunc.apply(ids[1])).add("id", ids[1])
        .build();
    Marker nov1 = new Marker.Builder()
        .add("year", 2013).add("month", 11).add("day", 11)
        .add("id-hash", hashFunc.apply(ids[1])).add("id", ids[1])
        .build();
    assertIterableEquals(Sets.newHashSet(
        new MarkerRange(cmp).from(sep0).to(nov0),
        new MarkerRange(cmp).from(sep1).to(nov1)),
        c.toKeyRanges(strategy));

    // more complication
    Marker sep2 = new Marker.Builder()
        .add("year", 2013).add("month", 9).add("day", 12)
        .add("id-hash", hashFunc.apply(ids[0])).add("id", ids[0])
        .add("component", "com.company.Main")
        .build();
    Marker nov2 = new Marker.Builder()
        .add("year", 2013).add("month", 11).add("day", 11)
        .add("id-hash", hashFunc.apply(ids[0])).add("id", ids[0])
        .add("component", "com.company.Main")
        .build();
    Marker sep3 = new Marker.Builder()
        .add("year", 2013).add("month", 9).add("day", 12)
        .add("id-hash", hashFunc.apply(ids[1])).add("id", ids[1])
        .add("component", "com.company.Main")
        .build();
    Marker nov3 = new Marker.Builder()
        .add("year", 2013).add("month", 11).add("day", 11)
        .add("id-hash", hashFunc.apply(ids[1])).add("id", ids[1])
        .add("component", "com.company.Main")
        .build();
    Marker sep4 = new Marker.Builder()
        .add("year", 2013).add("month", 9).add("day", 12)
        .add("id-hash", hashFunc.apply(ids[0])).add("id", ids[0])
        .add("component", "com.company.SomeClass")
        .build();
    Marker nov4 = new Marker.Builder()
        .add("year", 2013).add("month", 11).add("day", 11)
        .add("id-hash", hashFunc.apply(ids[0])).add("id", ids[0])
        .add("component", "com.company.SomeClass")
        .build();
    Marker sep5 = new Marker.Builder()
        .add("year", 2013).add("month", 9).add("day", 12)
        .add("id-hash", hashFunc.apply(ids[1])).add("id", ids[1])
        .add("component", "com.company.SomeClass")
        .build();
    Marker nov5 = new Marker.Builder()
        .add("year", 2013).add("month", 11).add("day", 11)
        .add("id-hash", hashFunc.apply(ids[1])).add("id", ids[1])
        .add("component", "com.company.SomeClass")
        .build();
    assertIterableEquals(Sets.newHashSet(
        new MarkerRange(cmp).from(sep2).to(nov2),
        new MarkerRange(cmp).from(sep3).to(nov3),
        new MarkerRange(cmp).from(sep4).to(nov4),
        new MarkerRange(cmp).from(sep5).to(nov5)),
        c.with("component", "com.company.Main", "com.company.SomeClass")
            .toKeyRanges(strategy));
  }

  @Test
  public void testOneSidedRange() {
    // Thu Sep 12 14:15:47 PDT 2013
    Constraints c = new Constraints().from("timestamp", 1379020547042L);

    Marker sep = new Marker.Builder()
        .add("year", 2013).add("month", 9).add("day", 12).build();
    assertIterableEquals(Sets.newHashSet(new MarkerRange(timeCmp).from(sep)),
        c.toKeyRanges(time));
  }

  public static <T> void assertIterableEquals(
      Iterable<T> expected, Iterable<T> actualIterable) {
    Set<T> expectedSet = Sets.newHashSet(expected);
    for (T actual : actualIterable) {
      // need to check as iteration happens because the object is reused
      Assert.assertTrue("Unexpected record: " + actual + " expected: " + expectedSet,
          expectedSet.remove(actual));
    }
    Assert.assertEquals("Not all expected records were present: " + expectedSet,
        0, expectedSet.size());
  }
}
