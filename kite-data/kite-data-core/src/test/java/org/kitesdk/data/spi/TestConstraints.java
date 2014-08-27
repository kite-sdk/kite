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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.spi.partition.HashFieldPartitioner;
import org.kitesdk.data.spi.predicates.Predicates;
import org.kitesdk.data.spi.predicates.Ranges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestConstraints {
  private static final Schema schema = SchemaBuilder.record("Event").fields()
      .requiredString("id")
      .requiredLong("timestamp")
      .requiredString("color")
      .endRecord();

  private static PartitionStrategy timeOnly = new PartitionStrategy.Builder()
      .year("timestamp")
      .month("timestamp")
      .day("timestamp")
      .build();

  private static PartitionStrategy strategy = new PartitionStrategy.Builder()
      .hash("id", "id-hash", 64)
      .year("timestamp")
      .month("timestamp")
      .day("timestamp")
      .identity("id")
      .build();

  private static final Constraints emptyConstraints =
      new Constraints(schema, strategy);

  private static final Logger LOG = LoggerFactory
      .getLogger(TestConstraints.class);

  public static final long START = System.currentTimeMillis();
  public static final Random RANDOM = new Random(1234);
  public static final long ONE_DAY_MILLIS = 86400000; // 24 * 60 * 60 * 1000

  public static class GenericEvent {
    public String id = UUID.randomUUID().toString();
    public long timestamp = START + RANDOM.nextInt(100000);

    public String getId() {
      return id;
    }

    public long getTimestamp() {
      return timestamp;
    }
  }

  public static final EntityAccessor<GenericEvent> accessor =
      DataModelUtil.accessor(GenericEvent.class, schema);

  @Test
  public void testEmptyConstraintsEntityPredicate() {
    Assert.assertNotNull("Empty constraints should produce an entity predicate",
        emptyConstraints.toEntityPredicate(accessor));
    Assert.assertTrue("Should match event",
        emptyConstraints.toEntityPredicate(accessor).apply(new GenericEvent()));
    Assert.assertFalse("Should not match null",
        emptyConstraints.toEntityPredicate(accessor).apply(null));

    Assert.assertNotNull("Should produce an unbound key range",
        emptyConstraints.toKeyRanges());

    Assert.assertTrue("Empty constraints should be unbounded",
        emptyConstraints.isUnbounded());
  }

  @Test
  public void testEmptyConstraintsKeyPredicateRequiresStrategy() {
    TestHelpers.assertThrows("Cannot produce a key Predicate without strategy",
        NullPointerException.class, new Runnable() {
          @Override
          public void run() {
            new Constraints(schema).toKeyPredicate();
          }
        });
  }

  @Test
  public void testEmptyConstraintsKeyPredicate() {
    Constraints constraints = emptyConstraints.partitionedBy(strategy);
    Predicate<StorageKey> keyPredicate = constraints.toKeyPredicate();
    Assert.assertNotNull("Should produce a key Predicate", keyPredicate);
    Assert.assertFalse("Should not match a null key", keyPredicate.apply(null));
    Assert.assertTrue("Should match empty key",
        keyPredicate.apply(new StorageKey(strategy)));
  }

  @Test
  public void testExists() {
    Constraints exists = emptyConstraints.with("id");

    Assert.assertEquals("Should be Predicates.exists()",
        Predicates.exists(), exists.get("id"));

    GenericEvent event = new GenericEvent();
    Assert.assertTrue("Should match event with non-null id",
        exists.toEntityPredicate(accessor).apply(event));

    event.id = null;
    Assert.assertFalse("Should not match event with null id",
        exists.toEntityPredicate(accessor).apply(event));

    Assert.assertNotNull("Should produce a key range",
        exists.toKeyRanges());

    Assert.assertFalse("Non-empty constraints (exists) should not be unbounded",
        exists.isUnbounded());
  }

  @Test
  public void testExistsRefinement() {
    Constraints exists = emptyConstraints.with("id");

    String id = UUID.randomUUID().toString();
    Assert.assertNotNull("Refine with exists should work",
        exists.with("id"));
    Assert.assertEquals("Refine with exists should do nothing",
        exists, exists.with("id"));
    Assert.assertNotNull("Refine with with should work",
        exists.with("id", id));
    Assert.assertEquals("Refine with with should produce with",
        Predicates.in(Sets.newHashSet(id)), exists.with("id", id).get("id"));
    Assert.assertNotNull("Refine with from should work",
        exists.from("id", "0"));
    Assert.assertEquals("Refine with from should produce Range",
        Ranges.atLeast("0"), exists.from("id", "0").get("id"));
    Assert.assertNotNull("Refine with fromAfter should work",
        exists.fromAfter("id", "0"));
    Assert.assertEquals("Refine with fromAfter should produce Range",
        Ranges.greaterThan("0"), exists.fromAfter("id", "0").get("id"));
    Assert.assertNotNull("Refine with to should work",
        exists.to("id", "0"));
    Assert.assertEquals("Refine with to should produce Range",
        Ranges.atMost("0"), exists.to("id", "0").get("id"));
    Assert.assertNotNull("Refine with toBefore should work",
        exists.toBefore("id", "0"));
    Assert.assertEquals("Refine with toBefore should produce Range",
        Ranges.lessThan("0"), exists.toBefore("id", "0").get("id"));
  }

  @Test(expected=NullPointerException.class)
  public void testWithNull() {
    emptyConstraints.with("id", (Object) null);
  }

  @Test
  public void testWithSingle() {
    String id = UUID.randomUUID().toString();
    Constraints withSingle = emptyConstraints.with("id", id);

    Assert.assertNotNull(withSingle);
    Assert.assertEquals("Should produce In(single)",
        Predicates.in(Sets.newHashSet(id)), withSingle.get("id"));

    GenericEvent event = new GenericEvent();
    Assert.assertFalse("Should not match event with incorrect id",
        withSingle.toEntityPredicate(accessor).apply(event));

    event.id = id;
    Assert.assertTrue("Should match event with correct id",
        withSingle.toEntityPredicate(accessor).apply(event));

    Assert.assertFalse("Non-empty constraints (with) should not be unbounded",
        withSingle.isUnbounded());
  }

  @Test
  public void testWithMultiple() {
    String[] ids = new String[] {
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString()};
    Constraints withMultiple = emptyConstraints.with("id", (Object[]) ids);

    Assert.assertNotNull(withMultiple);
    Assert.assertEquals("Should produce In(id0, id1)",
        Predicates.in(Sets.newHashSet(ids[0], ids[1])), withMultiple.get("id"));

    GenericEvent event = new GenericEvent();
    Assert.assertFalse("Should not match event with incorrect id",
        withMultiple.toEntityPredicate(accessor).apply(event));

    event.id = ids[0];
    Assert.assertTrue("Should match event with correct id",
        withMultiple.toEntityPredicate(accessor).apply(event));
    event.id = ids[1];
    Assert.assertTrue("Should match event with correct id",
        withMultiple.toEntityPredicate(accessor).apply(event));

    Assert.assertFalse("Non-empty constraints (with) should not be unbounded",
        withMultiple.isUnbounded());
  }

  @Test
  public void testWithRefinementUsingRange() {
    String[] ids = new String[] {
        "a", "b", "c"};
    final Constraints with = emptyConstraints.with("id", (Object[]) ids);

    Assert.assertNotNull(with);

    Assert.assertNotNull("Refine with from should work", with.from("id", "b"));
    Assert.assertEquals("Refine with from should produce subset",
        Predicates.in(Sets.newHashSet("b", "c")),
        with.from("id", "b").get("id"));
    Assert.assertNotNull("Refine with fromAfter should work",
        with.fromAfter("id", "b"));
    Assert.assertEquals("Refine with fromAfter should produce subset",
        Predicates.in(Sets.newHashSet("c")),
        with.fromAfter("id", "b").get("id"));
    Assert.assertNotNull("Refine with to should work",
        with.to("id", "b"));
    Assert.assertEquals("Refine with to should produce subset",
        Predicates.in(Sets.newHashSet("a", "b")),
        with.to("id", "b").get("id"));
    Assert.assertNotNull("Refine with toBefore should work",
        with.toBefore("id", "b"));
    Assert.assertEquals("Refine with toBefore should produce subset",
        Predicates.in(Sets.newHashSet("a")),
        with.toBefore("id", "b").get("id"));

    // this behavior is to avoid a two-sided comparison; we could change it
    TestHelpers.assertThrows("Should fail if endpoint not in set",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        with.to("id", "d");
      }
    });
  }

  @Test
  public void testWithRefinementUsingWith() {
    String[] ids = new String[] {
        "a", "b", "c"};
    final Constraints with = emptyConstraints.with("id", (Object[]) ids);

    Assert.assertNotNull(with);
    Assert.assertEquals("Exists should not change constraints",
        with, with.with("id"));

    Assert.assertNotNull("Should be able to refine with matching item",
        with.with("id", "a"));
    Assert.assertEquals(Predicates.in(Sets.newHashSet("a")),
        with.with("id", "a").get("id"));
    Assert.assertNotNull("Should be able to refine with subset",
        with.with("id", "b", "c"));
    Assert.assertEquals(Predicates.in(Sets.newHashSet("b", "c")),
        with.with("id", "b", "c").get("id"));

    TestHelpers.assertThrows("Should reject refinement outside set",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        with.with("id", "d");
      }
    });
  }

  @Test
  public void testRangeMethodsNull() {
    TestHelpers.assertThrows("from should fail with null value",
        NullPointerException.class, new Runnable() {
      @Override
      public void run() {
        emptyConstraints.from("id", null);
      }
    });
    TestHelpers.assertThrows("fromAfter should fail with null value",
        NullPointerException.class, new Runnable() {
      @Override
      public void run() {
        emptyConstraints.fromAfter("id", null);
      }
    });
    TestHelpers.assertThrows("to should fail with null value",
        NullPointerException.class, new Runnable() {
      @Override
      public void run() {
        emptyConstraints.to("id", null);
      }
    });
    TestHelpers.assertThrows("toBefore should fail with null value",
        NullPointerException.class, new Runnable() {
      @Override
      public void run() {
        emptyConstraints.toBefore("id", null);
      }
    });
  }

  @Test
  public void testBasicRanges() {
    // because we validate that the constraint is the guava range, no need to
    // test how it matches events
    Assert.assertEquals("Should be closed to pos-infinity",
        Ranges.atLeast(2013),
        emptyConstraints.from("year", 2013).get("year"));
    Assert.assertEquals("Should be open to pos-infinity",
        Ranges.greaterThan(2013),
        emptyConstraints.fromAfter("year", 2013).get("year"));
    Assert.assertEquals("Should be neg-infinity to closed",
        Ranges.atMost(2013),
        emptyConstraints.to("year", 2013).get("year"));
    Assert.assertEquals("Should be neg-infinity to open",
        Ranges.lessThan(2013),
        emptyConstraints.toBefore("year", 2013).get("year"));

    Assert.assertFalse("Non-empty constraints (from) should not be unbounded",
        emptyConstraints.from("year", 2013).isUnbounded());
    Assert.assertFalse("Non-empty constraints (fromAfter) should not be unbounded",
        emptyConstraints.fromAfter("year", 2013).isUnbounded());
    Assert.assertFalse("Non-empty constraints (to) should not be unbounded",
        emptyConstraints.to("year", 2013).isUnbounded());
    Assert.assertFalse("Non-empty constraints (toBefore) should not be unbounded",
        emptyConstraints.toBefore("year", 2013).isUnbounded());
  }

  @Test
  public void testBasicRangeRefinement() {
    Assert.assertEquals("Should be closed to closed",
        Ranges.closed(2012, 2014),
        emptyConstraints.from("year", 2012).to("year", 2014).get("year"));
    Assert.assertEquals("Should be closed to closed",
        Ranges.closed(2012, 2014),
        emptyConstraints.to("year", 2014).from("year", 2012).get("year"));
    Assert.assertEquals("Should be closed to open",
        Ranges.closedOpen(2012, 2014),
        emptyConstraints.from("year", 2012).toBefore("year", 2014).get("year"));
    Assert.assertEquals("Should be closed to open",
        Ranges.closedOpen(2012, 2014),
        emptyConstraints.toBefore("year", 2014).from("year", 2012).get("year"));
    Assert.assertEquals("Should be open to open",
        Ranges.open(2012, 2014),
        emptyConstraints.fromAfter("year", 2012).toBefore("year", 2014).get("year"));
    Assert.assertEquals("Should be open to open",
        Ranges.open(2012, 2014),
        emptyConstraints.toBefore("year", 2014).fromAfter("year", 2012).get("year"));
    Assert.assertEquals("Should be open to closed",
        Ranges.openClosed(2012, 2014),
        emptyConstraints.fromAfter("year", 2012).to("year", 2014).get("year"));
    Assert.assertEquals("Should be open to closed",
        Ranges.openClosed(2012, 2014),
        emptyConstraints.to("year", 2014).fromAfter("year", 2012).get("year"));
  }

  @Test
  public void testRangeEndpointRefinementUsingRange() {
    // using the current end-points
    final Constraints startRange = emptyConstraints.from("year", 2012).to("year", 2014);
    Assert.assertEquals(Ranges.closed(2012, 2014),
        startRange.to("year", 2014).get("year"));
    Assert.assertEquals(Ranges.closedOpen(2012, 2014),
        startRange.toBefore("year", 2014).get("year"));
    Assert.assertEquals(Ranges.closed(2012, 2014),
        startRange.from("year", 2012).get("year"));
    Assert.assertEquals(Ranges.openClosed(2012, 2014),
        startRange.fromAfter("year", 2012).get("year"));
    TestHelpers.assertThrows("Cannot change closed endpoint to open",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        startRange.toBefore("year", 2014).to("year", 2014);
      }
    });
    TestHelpers.assertThrows("Cannot change closed endpoint to open",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        startRange.fromAfter("year", 2012).from("year", 2012);
      }
    });
  }

  @Test
  public void testRangeMidpointRefinementUsingRange() {
    final Constraints startRange = emptyConstraints
        .from("year", 2012).toBefore("year", 2014);

    // any method can be used with a valid midpoint to limit the range
    Assert.assertEquals("Can refine using from",
        Ranges.closedOpen(2013, 2014),
        startRange.from("year", 2013).get("year"));
    Assert.assertEquals("Can refine using fromAfter",
        Ranges.open(2013, 2014),
        startRange.fromAfter("year", 2013).get("year"));
    Assert.assertEquals("Can refine using to",
        Ranges.closed(2012, 2013),
        startRange.to("year", 2013).get("year"));
    Assert.assertEquals("Can refine using toBefore",
        Ranges.closedOpen(2012, 2013),
        startRange.toBefore("year", 2013).get("year"));
  }

  @Test
  public void testRangeRefinementUsingWith() {
    final Constraints startRange = emptyConstraints
        .from("year", 2012).toBefore("year", 2014);

    // Can only refine using a subset
    Assert.assertEquals("Refinement with a set produces a set",
        Predicates.in(Sets.newHashSet(2012, 2013)),
        startRange.with("year", 2013, 2012).get("year"));

    TestHelpers.assertThrows("Cannot refine with a value outside the range",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        startRange.with("year", 2014);
      }
    });
  }

  @Test
  public void testRangeRefinementUsingExists() {
    final Constraints startRange = emptyConstraints
        .from("year", 2012).toBefore("year", 2014);
    Assert.assertEquals(startRange, startRange.with("year"));
  }

  @Test
  public void testBasicMatches() {
    GenericEvent e = new GenericEvent();
    StorageKey key = new StorageKey(strategy).reuseFor(e, accessor);

    Constraints time = emptyConstraints.partitionedBy(strategy)
        .from("timestamp", START)
        .to("timestamp", START + 100000);
    Predicate<StorageKey> matchKeys = time.toKeyPredicate();
    Predicate<GenericEvent> matchEvents = time.toEntityPredicate(accessor);
    Assert.assertTrue(matchKeys.apply(key));
    Assert.assertTrue(matchEvents.apply(e));

    Constraints timeAndUUID = time.with("id", e.getId());
    Assert.assertTrue(timeAndUUID.toKeyPredicate().apply(key));
    Assert.assertTrue(timeAndUUID.toEntityPredicate(accessor).apply(e));

    // just outside the actual range should match partition but not event
    e.timestamp = START - 1;
    key.reuseFor(e, accessor);
    Assert.assertFalse(time.toEntityPredicate(accessor).apply(e));
    Assert.assertFalse(timeAndUUID.toEntityPredicate(accessor).apply(e));
    Assert.assertTrue(time.toKeyPredicate().apply(key));
    Assert.assertTrue(timeAndUUID.toKeyPredicate().apply(key));

    // just outside the actual range should match partition but not event
    e.timestamp = START - 100001;
    key.reuseFor(e, accessor);
    Assert.assertFalse(time.toEntityPredicate(accessor).apply(e));
    Assert.assertFalse(timeAndUUID.toEntityPredicate(accessor).apply(e));
    Assert.assertTrue(time.toKeyPredicate().apply(key));
    Assert.assertTrue(timeAndUUID.toKeyPredicate().apply(key));

    // a different day will cause the partition to stop matching
    e.timestamp = START - ONE_DAY_MILLIS;
    key.reuseFor(e, accessor);
    Assert.assertFalse(time.toEntityPredicate(accessor).apply(e));
    Assert.assertFalse(timeAndUUID.toEntityPredicate(accessor).apply(e));
    Assert.assertFalse(time.toKeyPredicate().apply(key));
    Assert.assertFalse(timeAndUUID.toKeyPredicate().apply(key));
  }

  @Test
  public void testBasicKeyMinimization() {
    Schema schema = SchemaBuilder.record("TestRecord").fields()
        .requiredLong("created_at")
        .requiredString("color")
        .optionalInt("number")
        .endRecord();
    FieldPartitioner<Object, Integer> hash50 =
        new HashFieldPartitioner("name", 50);

    Constraints c = new Constraints(schema)
        .with("number", 7, 14, 21, 28, 35, 42, 49)
        .with("color", "green", "orange")
        .from("created_at",
            new DateTime(2013, 1, 1, 0, 0, DateTimeZone.UTC).getMillis())
        .toBefore("created_at",
            new DateTime(2014, 1, 1, 0, 0, DateTimeZone.UTC).getMillis());

    PartitionStrategy strategy = new PartitionStrategy.Builder()
        .hash("color", "hash", 50)
        .year("created_at").month("created_at").day("created_at")
        .identity("color")
        .build();

    StorageKey key = new StorageKey(strategy);

    key.replaceValues(Lists.<Object>newArrayList(
        hash50.apply("green"), 2013, 9, 1, "green"));
    Assert.assertEquals(Sets.newHashSet("number"),
        c.minimizeFor(key).keySet());

    // adjust the end time so that month is the last field checked
    Constraints c2 = c.toBefore("created_at",
        new DateTime(2013, 10, 1, 0, 0, DateTimeZone.UTC).getMillis());
    Assert.assertEquals(Sets.newHashSet("number"),
        c2.minimizeFor(key).keySet());

    // adjust the end time so that not all entities in the key are matched
    Constraints c3 = c.to("created_at",
        new DateTime(2013, 9, 1, 12, 0, DateTimeZone.UTC).getMillis());
    Assert.assertEquals(Sets.newHashSet("number", "created_at"),
        c3.minimizeFor(key).keySet());
  }

  @Test
  public void testAlignedWithPartitionBoundaries() {
    PartitionStrategy hashStrategy = new PartitionStrategy.Builder()
        .hash("id", "bucket", 32)
        .build();
    PartitionStrategy withColor = new PartitionStrategy.Builder()
        .range("color", "blue", "green", "red")
        .year("timestamp")
        .month("timestamp")
        .day("timestamp")
        .identity("id")
        .build();

    Constraints c = emptyConstraints.with("id", "a", "b", "c");

    // if the data is not partitioned by a field, then the partitioning cannot
    // be used to satisfy constraints for that field
    Assert.assertFalse("Cannot be satisfied by partition unless partitioned",
        c.with("color", "orange").partitionedBy(strategy).alignedWithBoundaries());
    // even if partitioned, lacking a strict projection means that we cannot
    // reason about what the predicate would accept given the partition data
    Assert.assertFalse("Cannot be satisfied by hash partition filtering",
        c.partitionedBy(hashStrategy).alignedWithBoundaries());
    Assert.assertFalse("Cannot be satisfied by time filtering for timestamp",
        c.with("timestamp",
            new DateTime(2013, 9, 1, 12, 0, DateTimeZone.UTC).getMillis())
            .partitionedBy(strategy).alignedWithBoundaries());
    // if there is a strict projection, we can only use it if it is equivalent
    // to the permissive projection. if we can't tell or if the strict
    // projection is too conservative, we can't use the partition data
    Assert.assertFalse("Cannot be satisfied because equality doesn't hold",
        c.with("color", "green", "brown").partitionedBy(withColor)
            .alignedWithBoundaries());
    Assert.assertFalse("Cannot be satisfied because equality doesn't hold",
        c.fromAfter("color", "blue").to("color", "red").partitionedBy(withColor)
            .alignedWithBoundaries());

    // if the strict projection and permissive projections are identical, then
    // we can conclude that the information in the partitions is sufficient to
    // satisfy the original predicate.
    Assert.assertTrue("Should be satisfied by partition filtering on id",
        c.partitionedBy(strategy).alignedWithBoundaries());
  }

  @Test
  public void testTimeAlignedWithPartitionBoundaries() {
    Constraints c = emptyConstraints.partitionedBy(strategy);
    Assert.assertFalse("Cannot be satisfied because timestamp is in middle of partition",
        c.from("timestamp", new DateTime(2013, 9, 1, 12, 0, DateTimeZone.UTC).getMillis())
            .alignedWithBoundaries());
    Assert.assertTrue("Should be satisfied because 'from' timestamp is on inclusive partition boundary",
        c.from("timestamp", new DateTime(2013, 9, 1, 0, 0, DateTimeZone.UTC).getMillis())
            .alignedWithBoundaries());
    Assert.assertFalse("Cannot be satisfied because 'from' timestamp is on exclusive partition boundary",
        c.fromAfter("timestamp", new DateTime(2013, 9, 1, 0, 0, DateTimeZone.UTC).getMillis())
            .alignedWithBoundaries());
    Assert.assertTrue("Should be satisfied because 'to' timestamp is on exclusive partition boundary",
        c.toBefore("timestamp", new DateTime(2013, 9, 1, 0, 0, DateTimeZone.UTC).getMillis())
            .alignedWithBoundaries());
    Assert.assertFalse("Cannot be satisfied because 'to' timestamp is on inclusive partition boundary",
        c.to("timestamp", new DateTime(2013, 9, 1, 0, 0, DateTimeZone.UTC).getMillis())
            .alignedWithBoundaries());
    Assert.assertTrue("Should be satisfied because 'from' timestamp is on inclusive " +
            "partition boundary and 'to' timestamp is on exclusive partition boundary",
        c.from("timestamp", new DateTime(2013, 9, 1, 0, 0, DateTimeZone.UTC).getMillis())
            .toBefore("timestamp", new DateTime(2013, 9, 2, 0, 0, DateTimeZone.UTC).getMillis())
            .alignedWithBoundaries());
  }

  @Test
  public void testTimeRangeEdgeCases() {
    final long oct_24_2013 = new DateTime(2013, 10, 24, 0, 0, DateTimeZone.UTC).getMillis();
    final long oct_25_2013 = new DateTime(2013, 10, 25, 0, 0, DateTimeZone.UTC).getMillis();
    final long oct_24_2013_end = new DateTime(2013, 10, 24, 23, 59, 59, 999, DateTimeZone.UTC).getMillis();
    //final long oct_24_2013_end = oct_25_2013 - 1;
    GenericEvent e = new GenericEvent();

    e.timestamp = oct_25_2013;
    StorageKey key = new StorageKey(timeOnly).reuseFor(e, accessor);

    Constraints empty = emptyConstraints.partitionedBy(timeOnly);

    Constraints c = empty.with("timestamp", oct_24_2013);
    Assert.assertFalse("Should not match", c.toKeyPredicate().apply(key));

    c = empty.toBefore("timestamp", oct_24_2013_end);
    LOG.info("Constraints: {}", c);

    e.timestamp = oct_25_2013;
    key.reuseFor(e, accessor);
    Assert.assertFalse("Should not match toBefore", c.toKeyPredicate().apply(key));
  }

  @Test
  public void testRejectsNonSchemaOrPartitionFields() {
    TestHelpers.assertThrows("Should reject unknown field name",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            emptyConstraints.with("prescription", 34);
          }
        });
  }

  @Test
  public void testRejectsFieldSchemaMismatch() {
    TestHelpers.assertThrows("Should reject incorrect schema, string for long",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            emptyConstraints.with("timestamp", "boat");
          }
        });
    TestHelpers.assertThrows("Should reject incorrect schema, int for string",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            emptyConstraints.with("color", 34);
          }
        });
  }

  @Test
  public void testRejectsPartitionFieldSchemaMismatch() {
    TestHelpers.assertThrows("Should reject incorrect schema, string for int",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            emptyConstraints.with("year", "joker");
          }
        });
    TestHelpers.assertThrows("Should reject incorrect schema, int for string",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            emptyConstraints.with("id", 34);
          }
        });
  }

  @Test
  public void testPartitionFieldConstraints() {
    GenericEvent e = new GenericEvent();
    DateTime eventDateTime = new Instant(e.getTimestamp())
        .toDateTime(DateTimeZone.UTC);

    // these constraints are for partition fields
    Constraints eventDay = emptyConstraints
        .with("year", eventDateTime.getYear())
        .with("month", eventDateTime.getMonthOfYear())
        .with("day", eventDateTime.getDayOfMonth());

    StorageKey eventKey = new StorageKey.Builder(strategy)
        .add("timestamp", e.getTimestamp())
        .add("id", e.getId())
        .build();

    Predicate<StorageKey> keyPredicate = eventDay.toKeyPredicate();
    Assert.assertNotNull("Should produce a KeyPredicate", keyPredicate);
    Assert.assertTrue("Should match key", keyPredicate.apply(eventKey));

    Predicate<GenericEvent> entityPredicate = eventDay
        .toEntityPredicate(accessor);
    Assert.assertNotNull("Should produce an EntityPredicate", entityPredicate);
    Assert.assertTrue("Should match entity", entityPredicate.apply(e));
  }

  @Test
  public void testPartitionFieldConstraintsMinimized() {
    GenericEvent e = new GenericEvent();
    DateTime eventDateTime = new Instant(e.getTimestamp())
        .toDateTime(DateTimeZone.UTC);

    // year and month constraints are for partition fields
    Constraints green = emptyConstraints
        .from("year", eventDateTime.getYear()) // will be removed
        .with("month", eventDateTime.getMonthOfYear()) // will be removed
        .with("color", "green");

    StorageKey eventKey = new StorageKey.Builder(strategy)
        .add("timestamp", e.getTimestamp())
        .add("id", e.getId())
        .build();

    Map<String, Predicate> minimized = green.minimizeFor(eventKey);
    Assert.assertEquals("Should have one unsatisfied constraint",
        1, minimized.size());
    Map.Entry<String, Predicate> entry = Iterables.getOnlyElement(
        minimized.entrySet());
    Assert.assertEquals("Should be the color constraint",
        "color", entry.getKey());
    Assert.assertEquals("Should match green",
        Predicates.in("green"), entry.getValue());
  }
}
