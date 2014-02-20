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
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;
import com.google.common.collect.Sets;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.data.FieldPartitioner;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.filesystem.DatasetTestUtilities;
import org.kitesdk.data.partition.DateFormatPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class TestConstraints {
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

  private PartitionStrategy timeOnly = new PartitionStrategy.Builder()
      .year("timestamp")
      .month("timestamp")
      .day("timestamp")
      .build();

  private PartitionStrategy strategy = new PartitionStrategy.Builder()
      .hash("id", "id-hash", 64)
      .year("timestamp")
      .month("timestamp")
      .day("timestamp")
      .identity("id", String.class, 100000)
      .build();

  @Test
  public void testDateFormatProjection() {
    // TODO: this belongs in a test for the DateFormatPartitioner
    FieldPartitioner<Long, String> fp =
        new DateFormatPartitioner("timestamp", "date", "yyyy-MM-dd");
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    format.setTimeZone(TimeZone.getTimeZone("UTC"));

    Predicate<String> result = fp.project(Ranges.open(START, START + ONE_DAY_MILLIS));
    Assert.assertEquals(format.format(new Date(START - 123)), ((Range) result).lowerEndpoint());
    Assert.assertEquals(format.format(new Date(START + ONE_DAY_MILLIS + 123)), ((Range) result).upperEndpoint());
  }

  @Test
  public void testEmptyConstraints() {
    Constraints empty = new Constraints();

    Predicate<StorageKey> keyPredicate = empty.toKeyPredicate();
    Assert.assertNotNull("Empty constraints should produce a key predicate",
        keyPredicate);
    // TODO: these should be in their own tests
    Assert.assertFalse("Should not match a null key", keyPredicate.apply(null));
    Assert.assertTrue("Should match empty time key",
        keyPredicate.apply(new StorageKey(timeOnly)));
    Assert.assertTrue("Should match empty strategy key",
        keyPredicate.apply(new StorageKey(strategy)));

    Assert.assertNotNull("Empty constraints should produce an entity predicate",
        empty.toEntityPredicate());
    Assert.assertTrue("Should match event",
        empty.toEntityPredicate().apply(new GenericEvent()));
    Assert.assertFalse("Should not match null",
        empty.toEntityPredicate().apply(null));

    Assert.assertNotNull("Should produce an unbound key range",
        empty.toKeyRanges(strategy));
  }

  @Test
  public void testExists() {
    Constraints exists = new Constraints().with("id");

    Assert.assertEquals("Should be Predicates.exists()",
        Predicates.exists(), exists.get("id"));

    GenericEvent event = new GenericEvent();
    Assert.assertTrue("Should match event with non-null id",
        exists.toEntityPredicate().apply(event));

    event.id = null;
    Assert.assertFalse("Should not match event with null id",
        exists.toEntityPredicate().apply(event));

    Assert.assertNotNull("Should produce a key predicate",
        exists.toKeyPredicate());
    Assert.assertNotNull("Should produce a key range",
        exists.toKeyRanges(strategy));
  }

  @Test
  public void testExistsRefinement() {
    Constraints exists = new Constraints().with("id");

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
    new Constraints().with("id", (Object) null);
  }

  @Test
  public void testWithSingle() {
    String id = UUID.randomUUID().toString();
    Constraints withSingle = new Constraints().with("id", id);

    Assert.assertNotNull(withSingle);
    Assert.assertEquals("Should produce In(single)",
        Predicates.in(Sets.newHashSet(id)), withSingle.get("id"));

    GenericEvent event = new GenericEvent();
    Assert.assertFalse("Should not match event with incorrect id",
        withSingle.toEntityPredicate().apply(event));

    event.id = id;
    Assert.assertTrue("Should match event with correct id",
        withSingle.toEntityPredicate().apply(event));
  }

  @Test
  public void testWithMultiple() {
    String[] ids = new String[] {
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString()};
    Constraints withMultiple = new Constraints().with("id", (Object[]) ids);

    Assert.assertNotNull(withMultiple);
    Assert.assertEquals("Should produce In(id0, id1)",
        Predicates.in(Sets.newHashSet(ids[0], ids[1])), withMultiple.get("id"));

    GenericEvent event = new GenericEvent();
    Assert.assertFalse("Should not match event with incorrect id",
        withMultiple.toEntityPredicate().apply(event));

    event.id = ids[0];
    Assert.assertTrue("Should match event with correct id",
        withMultiple.toEntityPredicate().apply(event));
    event.id = ids[1];
    Assert.assertTrue("Should match event with correct id",
        withMultiple.toEntityPredicate().apply(event));
  }

  @Test
  public void testWithRefinementUsingRange() {
    String[] ids = new String[] {
        "a", "b", "c"};
    final Constraints with = new Constraints().with("id", (Object[]) ids);

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
    final Constraints with = new Constraints().with("id", (Object[]) ids);

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
        new Constraints().from("id", null);
      }
    });
    TestHelpers.assertThrows("fromAfter should fail with null value",
        NullPointerException.class, new Runnable() {
      @Override
      public void run() {
        new Constraints().fromAfter("id", null);
      }
    });
    TestHelpers.assertThrows("to should fail with null value",
        NullPointerException.class, new Runnable() {
      @Override
      public void run() {
        new Constraints().to("id", null);
      }
    });
    TestHelpers.assertThrows("toBefore should fail with null value",
        NullPointerException.class, new Runnable() {
      @Override
      public void run() {
        new Constraints().toBefore("id", null);
      }
    });
  }

  @Test
  public void testBasicRanges() {
    // because we validate that the constraint is the guava range, no need to
    // test how it matches events
    Assert.assertEquals("Should be closed to pos-infinity",
        Ranges.atLeast(2013),
        new Constraints().from("year", 2013).get("year"));
    Assert.assertEquals("Should be open to pos-infinity",
        Ranges.greaterThan(2013),
        new Constraints().fromAfter("year", 2013).get("year"));
    Assert.assertEquals("Should be neg-infinity to closed",
        Ranges.atMost(2013),
        new Constraints().to("year", 2013).get("year"));
    Assert.assertEquals("Should be neg-infinity to open",
        Ranges.lessThan(2013),
        new Constraints().toBefore("year", 2013).get("year"));
  }

  @Test
  public void testBasicRangeRefinement() {
    Assert.assertEquals("Should be closed to closed",
        Ranges.closed(2012, 2014),
        new Constraints().from("year", 2012).to("year", 2014).get("year"));
    Assert.assertEquals("Should be closed to closed",
        Ranges.closed(2012, 2014),
        new Constraints().to("year", 2014).from("year", 2012).get("year"));
    Assert.assertEquals("Should be closed to open",
        Ranges.closedOpen(2012, 2014),
        new Constraints().from("year", 2012).toBefore("year", 2014).get("year"));
    Assert.assertEquals("Should be closed to open",
        Ranges.closedOpen(2012, 2014),
        new Constraints().toBefore("year", 2014).from("year", 2012).get("year"));
    Assert.assertEquals("Should be open to open",
        Ranges.open(2012, 2014),
        new Constraints().fromAfter("year", 2012).toBefore("year", 2014).get("year"));
    Assert.assertEquals("Should be open to open",
        Ranges.open(2012, 2014),
        new Constraints().toBefore("year", 2014).fromAfter("year", 2012).get("year"));
    Assert.assertEquals("Should be open to closed",
        Ranges.openClosed(2012, 2014),
        new Constraints().fromAfter("year", 2012).to("year", 2014).get("year"));
    Assert.assertEquals("Should be open to closed",
        Ranges.openClosed(2012, 2014),
        new Constraints().to("year", 2014).fromAfter("year", 2012).get("year"));
  }

  @Test
  public void testRangeEndpointRefinementUsingRange() {
    // using the current end-points
    final Constraints startRange = new Constraints().from("year", 2012).to("year", 2014);
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
    final Constraints startRange = new Constraints()
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
    final Constraints startRange = new Constraints()
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
    final Constraints startRange = new Constraints()
        .from("year", 2012).toBefore("year", 2014);
    Assert.assertEquals(startRange, startRange.with("year"));
  }

  @Test
  public void testBasicMatches() {
    GenericEvent e = new GenericEvent();
    StorageKey key = new StorageKey(strategy).reuseFor(e);

    Constraints time = new Constraints()
        .from("timestamp", START)
        .to("timestamp", START + 100000);
    Predicate<StorageKey> matchKeys = time.toKeyPredicate();
    Predicate<GenericEvent> matchEvents = time.toEntityPredicate();
    Assert.assertTrue(matchKeys.apply(key));
    Assert.assertTrue(matchEvents.apply(e));

    Constraints timeAndUUID = time.with("id", e.getId());
    Assert.assertTrue(timeAndUUID.toKeyPredicate().apply(key));
    Assert.assertTrue(timeAndUUID.toEntityPredicate().apply(e));

    // just outside the actual range should match partition but not event
    e.timestamp = START - 1;
    key.reuseFor(e);
    Assert.assertFalse(time.toEntityPredicate().apply(e));
    Assert.assertFalse(timeAndUUID.toEntityPredicate().apply(e));
    Assert.assertTrue(time.toKeyPredicate().apply(key));
    Assert.assertTrue(timeAndUUID.toKeyPredicate().apply(key));

    // just outside the actual range should match partition but not event
    e.timestamp = START - 100001;
    key.reuseFor(e);
    Assert.assertFalse(time.toEntityPredicate().apply(e));
    Assert.assertFalse(timeAndUUID.toEntityPredicate().apply(e));
    Assert.assertTrue(time.toKeyPredicate().apply(key));
    Assert.assertTrue(timeAndUUID.toKeyPredicate().apply(key));

    // a different day will cause the partition to stop matching
    e.timestamp = START - ONE_DAY_MILLIS;
    key.reuseFor(e);
    Assert.assertFalse(time.toEntityPredicate().apply(e));
    Assert.assertFalse(timeAndUUID.toEntityPredicate().apply(e));
    Assert.assertFalse(time.toKeyPredicate().apply(key));
    Assert.assertFalse(timeAndUUID.toKeyPredicate().apply(key));
  }

  @Test
  public void testTimeRangeEdgeCases() {
    final long oct_24_2013 = new DateTime(2013, 10, 24, 0, 0, DateTimeZone.UTC).getMillis();
    final long oct_25_2013 = new DateTime(2013, 10, 25, 0, 0, DateTimeZone.UTC).getMillis();
    final long oct_24_2013_end = new DateTime(2013, 10, 24, 23, 59, 59, 999, DateTimeZone.UTC).getMillis();
    //final long oct_24_2013_end = oct_25_2013 - 1;
    GenericEvent e = new GenericEvent();

    e.timestamp = oct_25_2013;
    StorageKey key = new StorageKey(timeOnly).reuseFor(e);

    Constraints c = new Constraints().with("timestamp", oct_24_2013);
    Assert.assertFalse("Should not match", c.toKeyPredicate().apply(key));

    c = new Constraints().toBefore("timestamp", oct_24_2013_end);
    LOG.info("Constraints: {}", c);

    e.timestamp = oct_25_2013;
    key.reuseFor(e);
    Assert.assertFalse("Should not match toBefore", c.toKeyPredicate().apply(key));
  }

  @Test
  public void testInSerialization() throws IOException, ClassNotFoundException {
    Constraints inConstraint = new Constraints();
    String propertyName = "name";
    inConstraint.with(propertyName, "abc");

    Constraints newIn = serializeAndDeserialize(inConstraint);

    Predicate predicate = newIn.get(propertyName);
    assertThat(predicate, is(inConstraint.get(propertyName)));
  }

  @Test
  public void testInAvroSerialization() throws IOException, ClassNotFoundException {
    Constraints inConstraint = new Constraints();
    String propertyName = "name";

    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(new Schema.Parser().parse(new File(DatasetTestUtilities.USER_SCHEMA_URL)))
        .set("username", "test").set("email", "email@example.com");
    inConstraint.with(propertyName, recordBuilder.build());

    Constraints newIn = serializeAndDeserialize(inConstraint);

    Predicate predicate = newIn.get(propertyName);
    assertThat(predicate, is(inConstraint.get(propertyName)));
  }

  @Test
  public void testInAvroReflectSerialization() throws IOException, ClassNotFoundException {
    Constraints inConstraint = new Constraints();
    String propertyName = "name";

    SimpleUser user = new SimpleUser();
    user.setEmail("john@example.com");
    user.setName("John Doe");

    inConstraint.with(propertyName, user);

    Constraints newIn = serializeAndDeserialize(inConstraint);

    Predicate predicate = newIn.get(propertyName);
    assertThat(predicate, is(inConstraint.get(propertyName)));
  }

  @Test
  public void testExistsSerialization() throws IOException, ClassNotFoundException {
    Constraints exists = new Constraints();
    String propertyName = "name";
    exists.with(propertyName, new Object[0]);

    Constraints newExists = serializeAndDeserialize(exists);

    Predicate predicate = newExists.get(propertyName);
    assertThat(predicate, is(exists.get(propertyName)));
  }

  @Test
  public void testRangeAvroSerialization() throws IOException, ClassNotFoundException {
    Constraints rangeConstraint = new Constraints();
    String propertyName = "name";

    GenericRecordBuilder recordBuilder =
        new GenericRecordBuilder(new Schema.Parser().parse(new File(DatasetTestUtilities.USER_SCHEMA_URL)))
        .set("username", "test").set("email", "email@example.com");
    rangeConstraint.from(propertyName, recordBuilder.build());

    Constraints newIn = serializeAndDeserialize(rangeConstraint);

    Predicate predicate = newIn.get(propertyName);
    assertThat(predicate, is(rangeConstraint.get(propertyName)));
  }

  @Test
  public void testRangeFromSerialization() throws IOException, ClassNotFoundException {
    Constraints range = new Constraints();
    String propertyName = "name";
    range.from(propertyName, 10L);

    Constraints newRange = serializeAndDeserialize(range);

    Predicate predicate = newRange.get(propertyName);
    assertThat(predicate, is(range.get(propertyName)));
  }

  @Test
  public void testRangeFromAfterSerialization() throws IOException, ClassNotFoundException {
    Constraints range = new Constraints();
    String propertyName = "name";
    range.fromAfter(propertyName, 10L);

    Constraints newRange = serializeAndDeserialize(range);

    Predicate predicate = newRange.get(propertyName);
    assertThat(predicate, is(range.get(propertyName)));
  }


  @Test
  public void testRangeToSerialization() throws IOException, ClassNotFoundException {
    Constraints range = new Constraints();
    String propertyName = "name";
    range.to(propertyName, 10L);

    Constraints newRange = serializeAndDeserialize(range);

    Predicate predicate = newRange.get(propertyName);
    assertThat(predicate, is(range.get(propertyName)));
  }

  @Test
  public void testRangeToBeforeSerialization() throws IOException, ClassNotFoundException {
    Constraints range = new Constraints();
    String propertyName = "name";
    range.toBefore(propertyName, 10L);

    Constraints newRange = serializeAndDeserialize(range);

    Predicate predicate = newRange.get(propertyName);
    assertThat(predicate, is(range.get(propertyName)));
  }

  @Test
  public void testFromToSerialization() throws IOException, ClassNotFoundException {
    Constraints range = new Constraints();
    String propertyName = "name";
    range.from(propertyName, 1L).to(propertyName, 10L);

    Constraints newRange = serializeAndDeserialize(range);

    Predicate predicate = newRange.get(propertyName);
    assertThat(predicate, is(range.get(propertyName)));
  }

  @Test
  public void testFromToBeforeSerialization() throws IOException, ClassNotFoundException {
    Constraints range = new Constraints();
    String propertyName = "name";
    range.from(propertyName, 1L).toBefore(propertyName, 10L);

    Constraints newRange = serializeAndDeserialize(range);

    Predicate predicate = newRange.get(propertyName);
    assertThat(predicate, is(range.get(propertyName)));
  }

  @Test
  public void testFromAfterToSerialization() throws IOException, ClassNotFoundException {
    Constraints range = new Constraints();
    String propertyName = "name";
    range.from(propertyName, 1L).to(propertyName, 10L);

    Constraints newRange = serializeAndDeserialize(range);

    Predicate predicate = newRange.get(propertyName);
    assertThat(predicate, is(range.get(propertyName)));
  }

  @Test
  public void testFromAfterToBeforeSerialization() throws IOException, ClassNotFoundException {
    Constraints range = new Constraints();
    String propertyName = "name";
    range.fromAfter(propertyName, 1L).toBefore(propertyName, 10L);

    Constraints newRange = serializeAndDeserialize(range);

    Predicate predicate = newRange.get(propertyName);
    assertThat(predicate, is(range.get(propertyName)));
  }

  @Test
  public void testMultiplePredicatesSerialization() throws IOException, ClassNotFoundException {
    Constraints constraints = new Constraints();
    String rangeName = "rangeName";
    constraints.fromAfter(rangeName, 1L).toBefore(rangeName, 10L);
    String existsName = "existsName";
    constraints.with(existsName);
    String inName = "inName";
    constraints.with(inName, "hello");

    Constraints newConstraints = serializeAndDeserialize(constraints);
    assertThat(newConstraints.get(rangeName), is(constraints.get(rangeName)));
    assertThat(newConstraints.get(existsName), is(constraints.get(existsName)));
    assertThat(newConstraints.get(inName), is(constraints.get(inName)));
  }

  public Constraints serializeAndDeserialize(Constraints constraints) throws IOException, ClassNotFoundException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(baos);

    out.writeObject(constraints);

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream in = new ObjectInputStream(bais);

    return (Constraints) in.readObject();
  }

  public static class SimpleUser {

    private String name;
    private String email;

    public String getName() {
      return name;
    }

    public void setName(final String name) {
      this.name = name;
    }

    public String getEmail() {
      return email;
    }

    public void setEmail(final String email) {
      this.email = email;
    }

    @Override
    public boolean equals(final Object o) {
      SimpleUser that = (SimpleUser) o;
      return this.email.equals(that.email) && this.name.equals(that.name);
    }

    @Override
    public int hashCode() {
      int result = 31*email.hashCode();
      result = 31*result+name.hashCode();
      return result;
    }
  }

}
