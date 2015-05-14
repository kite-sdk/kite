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

import com.google.common.io.Closeables;

import java.io.IOException;
import java.util.concurrent.Callable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.kitesdk.data.*;
import org.kitesdk.data.event.StandardEvent;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class TestRefinableViews extends MiniDFSTest {

  protected static final long now = System.currentTimeMillis();
  protected static final StandardEvent event = StandardEvent.newBuilder()
      .setEventInitiator("TestRefinableViews")
      .setEventName("TestEvent")
      .setUserId(0)
      .setSessionId("session-0")
      .setIp("localhost")
      .setTimestamp(now + 35405168L)
      .build();
  protected static final StandardEvent sepEvent = StandardEvent
      .newBuilder(event)
      .setTimestamp(1379020547042L) // Thu Sep 12 14:15:47 PDT 2013
      .build();
  protected static final StandardEvent octEvent = StandardEvent
      .newBuilder(event)
      .setTimestamp(1381612547042L) // Sat Oct 12 14:15:47 PDT 2013
      .setUserId(1)
      .build();
  protected static final StandardEvent novEvent = StandardEvent
      .newBuilder(event)
      .setTimestamp(1384204547042L) // Mon Nov 11 13:15:47 PST 2013
      .build();

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] {
        { false },  // default to local FS
        { true } }; // default to distributed FS
    return Arrays.asList(data);
  }

  // whether this should use the DFS
  private final boolean distributed;

  protected TestRefinableViews(boolean distributed) {
    this.distributed = distributed;
  }

  // from subclasses
  protected DatasetRepository repo = null;

  public abstract DatasetRepository newRepo();

  protected Configuration conf = null;
  protected FileSystem fs;
  protected PartitionStrategy strategy = null;
  protected DatasetDescriptor testDescriptor = null;
  protected RefinableView<StandardEvent> unbounded = null;

  @Before
  public void setup() throws Exception {
    this.conf = (distributed ?
        MiniDFSTest.getConfiguration() :
        new Configuration());
    this.fs = FileSystem.get(conf);

    this.repo = newRepo();
    this.strategy = new PartitionStrategy.Builder()
        .year("timestamp")
        .month("timestamp")
        .day("timestamp")
        .build();
    this.testDescriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:standard_event.avsc")
        .partitionStrategy(strategy)
        .build();
    repo.delete("ns", "test");
    this.unbounded = repo.create("ns", "test", testDescriptor);
  }

  public static <E> void assertContentEquals(Set<E> expected, View<E> view) throws IOException {
    DatasetReader<E> reader = null;
    try {
      reader = view.newReader();
      Assert.assertEquals(expected,
          Sets.newHashSet((Iterable<E>) reader));
    } finally {
      Closeables.close(reader, false);
    }
  }

  @Test(expected = ValidationException.class)
  public void testNonExistentField() {
    unbounded.with("nosuchfield");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInconsistentFieldType() {
    unbounded.with("timestamp", 0); // not '0L'
  }

  @Test
  public void testEmptyCheck() throws IOException {
    Assert.assertTrue("New dataset should be empty", unbounded.isEmpty());

    // NOTE: this is an un-restricted write so all should succeed
    DatasetWriter<StandardEvent> writer = null;
    try {
      writer = unbounded.newWriter();
      writer.write(sepEvent);
    } finally {
      Closeables.close(writer, false);
    }

    Assert.assertFalse("Should not be empty after write", unbounded.isEmpty());

    Assert.assertFalse("Should find event in September",
        unbounded.with("timestamp", sepEvent.getTimestamp()).isEmpty());

    Assert.assertTrue("Should not find event in October",
        unbounded.with("timestamp", octEvent.getTimestamp()).isEmpty());
  }

  @Test
  public void testLimitedReader() throws IOException {
    // NOTE: this is an un-restricted write so all should succeed
    DatasetWriter<StandardEvent> writer = null;
    try {
      writer = unbounded.newWriter();
      writer.write(sepEvent);
      writer.write(octEvent);
      writer.write(novEvent);
    } finally {
      Closeables.close(writer, false);
    }

    // unbounded
    assertContentEquals(Sets.newHashSet(sepEvent, octEvent, novEvent),
        unbounded);

    long octInstant = octEvent.getTimestamp();

    // single bound
    assertContentEquals(Sets.newHashSet(octEvent),
        unbounded.with("timestamp", octInstant));
    assertContentEquals(Sets.newHashSet(octEvent, novEvent),
        unbounded.from("timestamp", octInstant));
    assertContentEquals(Sets.newHashSet(sepEvent, octEvent),
        unbounded.to("timestamp", octInstant));
    // these two test that the constraints are applied after the partitions are
    // filtered. the partition matches, but the events do not
    assertContentEquals(Sets.newHashSet(novEvent),
        unbounded.fromAfter("timestamp", octInstant));
    assertContentEquals(Sets.newHashSet(sepEvent),
        unbounded.toBefore("timestamp", octInstant));

    // in range
    long octStart = new DateTime(2013, 10, 1, 0, 0, DateTimeZone.UTC).getMillis();
    long novStart = new DateTime(2013, 11, 1, 0, 0, DateTimeZone.UTC).getMillis();
    assertContentEquals(Sets.<StandardEvent>newHashSet(octEvent),
        unbounded.from("timestamp", octStart).toBefore("timestamp", novStart));

    // with
    // this tests that non-key partition fields are filtered (user_id is not a part of
    // the partition strategy)
    assertContentEquals(Sets.newHashSet(sepEvent, novEvent),
        unbounded.with("user_id", 0L));
  }

  @Test
  public void testLimitedWriter() throws IOException {
    long start = new DateTime(2013, 10, 1, 0, 0, DateTimeZone.UTC).getMillis();
    long end = new DateTime(2013, 11, 14, 0, 0, DateTimeZone.UTC).getMillis();
    final RefinableView<StandardEvent> range = unbounded
        .from("timestamp", start).toBefore("timestamp", end);

    DatasetWriter<StandardEvent> writer = null;
    try {
      writer = range.newWriter();
      writer.write(octEvent);
      writer.write(novEvent);
    } finally {
      Closeables.close(writer, false);
    }
    assertContentEquals(Sets.newHashSet(octEvent, novEvent), range);

    TestHelpers.assertThrows("Should reject older event",
        IllegalArgumentException.class, new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        DatasetWriter<StandardEvent> writer = null;
        try {
          writer = range.newWriter();
          writer.write(sepEvent);
        } finally {
          Closeables.close(writer, false);
        }
        return null;
      }
    });
    TestHelpers.assertThrows("Should reject current event",
        IllegalArgumentException.class, new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        DatasetWriter<StandardEvent> writer = null;
        try {
          writer = range.newWriter();
          writer.write(event);
        } finally {
          Closeables.close(writer, false);
        }
        return null;
      }
    });
  }

  @Test
  public void testLimitedWriterForNonPartitionedField() throws IOException {
    final RefinableView<StandardEvent> view = unbounded.with("user_id", 0L);

    DatasetWriter<StandardEvent> writer = null;
    try {
      writer = view.newWriter();
      writer.write(sepEvent);
      writer.write(novEvent);
    } finally {
      Closeables.close(writer, false);
    }
    assertContentEquals(Sets.newHashSet(sepEvent, novEvent), view);

    TestHelpers.assertThrows("Should reject event with user_id = 1",
        IllegalArgumentException.class, new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        DatasetWriter<StandardEvent> writer = null;
        try {
          writer = view.newWriter();
          writer.write(octEvent);
        } finally {
          Closeables.close(writer, false);
        }
        return null;
      }
    });
  }

  @Test
  public void testFromView() {
    long instant = new DateTime(2013, 10, 1, 0, 0, DateTimeZone.UTC).getMillis();
    long later = instant + 1;
    final long earlier = instant - 1;
    final RefinableView<StandardEvent> fromOctober =
        unbounded.from("timestamp", instant);

    // test events
    Assert.assertFalse("Should not include older event",
        fromOctober.includes(sepEvent));
    Assert.assertTrue("Should include event",
        fromOctober.includes(octEvent));
    Assert.assertTrue("Should include newer event",
        fromOctober.includes(novEvent));
    Assert.assertTrue("Should include current event",
        fromOctober.includes(event));

    // special case: to(same instant)
    Assert.assertNotNull("to(same instant) should succeed",
        fromOctober.to("timestamp", instant));

    // test limiting with other view methods
    Assert.assertNotNull("adding independent constraints should succeed",
        fromOctober.with("event_name", "Event"));
    Assert.assertNotNull("with(later instant) should succeed",
        fromOctober.with("timestamp", later));
    Assert.assertNotNull("from(later instant) should succeed",
        fromOctober.from("timestamp", later));
    Assert.assertNotNull("fromAfter(later instant) should succeed",
        fromOctober.fromAfter("timestamp", later));
    Assert.assertNotNull("to(later instant) should succeed",
        fromOctober.to("timestamp", later));
    Assert.assertNotNull("toAfter(later instant) should succeed",
        fromOctober.toBefore("timestamp", later));

    // should fail when constraints produce an empty set
    TestHelpers.assertThrows("with(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        fromOctober.with("timestamp", earlier);
      }
    });
    TestHelpers.assertThrows("to(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        fromOctober.to("timestamp", earlier);
      }
    });
    TestHelpers.assertThrows("toBefore(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        fromOctober.toBefore("timestamp", earlier);
      }
    });

    // this behavior is debatable, but something odd is happening
    TestHelpers.assertThrows("from(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        fromOctober.from("timestamp", earlier);
      }
    });
    TestHelpers.assertThrows("fromAfter(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        fromOctober.fromAfter("timestamp", earlier);
      }
    });
  }

  @Test
  public void testFromAfterView() {
    final long instant = new DateTime(2013, 9, 30, 12, 59, 59, 999, DateTimeZone.UTC).getMillis();
    long later = instant + 1;
    final long earlier = instant - 1;
    final RefinableView<StandardEvent> afterSep =
        unbounded.fromAfter("timestamp", instant);

    // test events
    Assert.assertFalse("Should not include older event",
        afterSep.includes(sepEvent));
    Assert.assertTrue("Should include event",
        afterSep.includes(octEvent));
    Assert.assertTrue("Should include newer event",
        afterSep.includes(novEvent));
    Assert.assertTrue("Should include current event",
        afterSep.includes(event));

    // special case: to(same instant)
    TestHelpers.assertThrows("to(same instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        afterSep.to("timestamp", instant);
      }
    });

    // test limiting with other view methods
    Assert.assertNotNull("adding independent constraints should succeed",
        afterSep.with("event_name", "Event"));
    Assert.assertNotNull("with(later instant) should succeed",
        afterSep.with("timestamp", later));
    Assert.assertNotNull("from(later instant) should succeed",
        afterSep.from("timestamp", later));
    Assert.assertNotNull("fromAfter(later instant) should succeed",
        afterSep.fromAfter("timestamp", later));
    Assert.assertNotNull("to(later instant) should succeed",
        afterSep.to("timestamp", later));
    Assert.assertNotNull("toAfter(later instant) should succeed",
        afterSep.toBefore("timestamp", later));

    // should fail when constraints produce an empty set
    TestHelpers.assertThrows("with(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        afterSep.with("timestamp", earlier);
      }
    });
    TestHelpers.assertThrows("to(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        afterSep.to("timestamp", earlier);
      }
    });
    TestHelpers.assertThrows("toBefore(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        afterSep.toBefore("timestamp", earlier);
      }
    });

    // this behavior is debatable, but something odd is happening
    TestHelpers.assertThrows("from(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        afterSep.from("timestamp", earlier);
      }
    });
    TestHelpers.assertThrows("fromAfter(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        afterSep.fromAfter("timestamp", earlier);
      }
    });
  }

  @Test
  public void testToView() {
    final long instant = new DateTime(2013, 9, 30, 12, 59, 59, 999, DateTimeZone.UTC).getMillis();
    final long later = instant + 1;
    long earlier = instant - 1;
    final RefinableView<StandardEvent> toOct =
        unbounded.to("timestamp", instant);

    // test events
    Assert.assertTrue("Should include older event",
        toOct.includes(sepEvent));
    Assert.assertFalse("Should not include event",
        toOct.includes(octEvent));
    Assert.assertFalse("Should not include newer event",
        toOct.includes(novEvent));
    Assert.assertFalse("Should not include current event",
        toOct.includes(event));

    // special case: from(same instant)
    Assert.assertNotNull("from(same instant) should succeed",
        toOct.from("timestamp", instant));

    // test limiting with other view methods
    Assert.assertNotNull("adding independent constraints should succeed",
        toOct.with("event_name", "Event"));
    Assert.assertNotNull("with(earlier instant) should succeed",
        toOct.with("timestamp", earlier));
    Assert.assertNotNull("from(earlier instant) should succeed",
        toOct.from("timestamp", earlier));
    Assert.assertNotNull("fromAfter(earlier instant) should succeed",
        toOct.fromAfter("timestamp", earlier));
    Assert.assertNotNull("to(earlier instant) should succeed",
        toOct.to("timestamp", earlier));
    Assert.assertNotNull("toAfter(earlier instant) should succeed",
        toOct.toBefore("timestamp", earlier));

    // should fail when constraints produce an empty set
    TestHelpers.assertThrows("with(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        toOct.with("timestamp", later);
      }
    });
    TestHelpers.assertThrows("to(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        toOct.to("timestamp", later);
      }
    });
    TestHelpers.assertThrows("toBefore(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        toOct.toBefore("timestamp", later);
      }
    });

    // this behavior is debatable, but something odd is happening
    TestHelpers.assertThrows("from(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        toOct.from("timestamp", later);
      }
    });
    TestHelpers.assertThrows("fromAfter(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        toOct.fromAfter("timestamp", later);
      }
    });
  }

  @Test
  public void testToBeforeView() {
    final long instant = new DateTime(2013, 10, 1, 0, 0, DateTimeZone.UTC).getMillis();
    final long later = instant + 1;
    long earlier = instant - 1;
    final RefinableView<StandardEvent> beforeOct =
        unbounded.toBefore("timestamp", instant);

    // test events
    Assert.assertTrue("Should include older event",
        beforeOct.includes(sepEvent));
    Assert.assertFalse("Should not include event",
        beforeOct.includes(octEvent));
    Assert.assertFalse("Should not include newer event",
        beforeOct.includes(novEvent));
    Assert.assertFalse("Should not include current event",
        beforeOct.includes(event));

    // special case: from(same instant)
    TestHelpers.assertThrows("from(same instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        beforeOct.from("timestamp", instant);
      }
    });

    // test limiting with other view methods
    Assert.assertNotNull("adding independent constraints should succeed",
        beforeOct.with("event_name", "Event"));
    Assert.assertNotNull("with(earlier instant) should succeed",
        beforeOct.with("timestamp", earlier));
    Assert.assertNotNull("from(earlier instant) should succeed",
        beforeOct.from("timestamp", earlier));
    Assert.assertNotNull("fromAfter(earlier instant) should succeed",
        beforeOct.fromAfter("timestamp", earlier));
    Assert.assertNotNull("to(earlier instant) should succeed",
        beforeOct.to("timestamp", earlier));
    Assert.assertNotNull("toAfter(earlier instant) should succeed",
        beforeOct.toBefore("timestamp", earlier));

    // should fail when constraints produce an empty set
    TestHelpers.assertThrows("with(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        beforeOct.with("timestamp", later);
      }
    });
    TestHelpers.assertThrows("to(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        beforeOct.to("timestamp", later);
      }
    });
    TestHelpers.assertThrows("toBefore(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        beforeOct.toBefore("timestamp", later);
      }
    });

    // this behavior is debatable, but something odd is happening
    TestHelpers.assertThrows("from(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        beforeOct.from("timestamp", later);
      }
    });
    TestHelpers.assertThrows("fromAfter(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        beforeOct.fromAfter("timestamp", later);
      }
    });
  }

  @Test
  public void testWithView() {
    final long instant = new DateTime(2013, 10, 1, 0, 0, DateTimeZone.UTC).getMillis();
    final long later = instant + 1;
    final long earlier = instant - 1;
    final RefinableView<StandardEvent> withSpecificTimestamp =
        unbounded.with("timestamp", instant);

    Assert.assertNotNull("from(same instant) should succeed",
        withSpecificTimestamp.from("timestamp", instant));
    Assert.assertNotNull("to(same instant) should succeed",
        withSpecificTimestamp.to("timestamp", instant));
    Assert.assertNotNull("with(same instant) should succeed",
        withSpecificTimestamp.with("timestamp", instant));

    TestHelpers.assertThrows("with(different instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        withSpecificTimestamp.with("timestamp", later);
      }
    });
    TestHelpers.assertThrows("toBefore(same instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        withSpecificTimestamp.toBefore("timestamp", instant);
      }
    });
    TestHelpers.assertThrows("fromAfter(same instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        withSpecificTimestamp.fromAfter("timestamp", instant);
      }
    });
    TestHelpers.assertThrows("to(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        withSpecificTimestamp.to("timestamp", earlier);
      }
    });
    TestHelpers.assertThrows("from(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        withSpecificTimestamp.from("timestamp", later);
      }
    });
  }

  @Test
  public void testRange() {
    long start = new DateTime(2013, 10, 1, 0, 0, DateTimeZone.UTC).getMillis();
    long end = new DateTime(2013, 11, 1, 0, 0, DateTimeZone.UTC).getMillis();
    final long later = end + 1;
    final long earlier = start - 1;
    long included = octEvent.getTimestamp();
    final RefinableView<StandardEvent> oct = unbounded
        .from("timestamp", start).to("timestamp", end);

    // test events
    Assert.assertFalse("Should not include older event",
        oct.includes(sepEvent));
    Assert.assertTrue("Should include event",
        oct.includes(octEvent));
    Assert.assertFalse("Should not include newer event",
        oct.includes(novEvent));
    Assert.assertFalse("Should not include current event",
        oct.includes(event));

    // special cases
    Assert.assertNotNull("to(start) should succeed",
        oct.to("timestamp", start));
    Assert.assertNotNull("from(end) should succeed",
        oct.from("timestamp", end));

    // test limiting with other view methods
    Assert.assertNotNull("adding independent constraints should succeed",
        oct.with("event_name", "Event"));
    Assert.assertNotNull("with(included instant) should succeed",
        oct.with("timestamp", included));
    Assert.assertNotNull("from(included instant) should succeed",
        oct.from("timestamp", included));
    Assert.assertNotNull("fromAfter(included instant) should succeed",
        oct.fromAfter("timestamp", included));
    Assert.assertNotNull("to(included instant) should succeed",
        oct.to("timestamp", included));
    Assert.assertNotNull("toAfter(included instant) should succeed",
        oct.toBefore("timestamp", included));

    // should fail when constraints produce an empty set
    TestHelpers.assertThrows("with(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        oct.with("timestamp", earlier);
      }
    });
    TestHelpers.assertThrows("to(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        oct.to("timestamp", earlier);
      }
    });
    TestHelpers.assertThrows("toBefore(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        oct.toBefore("timestamp", earlier);
      }
    });

    // this behavior is debatable, but something odd is happening
    TestHelpers.assertThrows("from(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        oct.from("timestamp", earlier);
      }
    });
    TestHelpers.assertThrows("fromAfter(earlier instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        oct.fromAfter("timestamp", earlier);
      }
    });

    // should fail when constraints produce an empty set
    TestHelpers.assertThrows("with(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        oct.with("timestamp", later);
      }
    });
    TestHelpers.assertThrows("to(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        oct.to("timestamp", later);
      }
    });
    TestHelpers.assertThrows("toBefore(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        oct.toBefore("timestamp", later);
      }
    });

    // this behavior is debatable, but something odd is happening
    TestHelpers.assertThrows("from(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        oct.from("timestamp", later);
      }
    });
    TestHelpers.assertThrows("fromAfter(later instant) should fail",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        oct.fromAfter("timestamp", later);
      }
    });
  }

  @Test
  public void testUnboundedView() {
    // test events
    Assert.assertTrue("Should include any StandardEvent",
        unbounded.includes(event));
    Assert.assertTrue("Should include even null events",
        unbounded.includes(null));
    Assert.assertTrue("Should include older event",
        unbounded.includes(sepEvent));
    Assert.assertTrue("Should include event",
        unbounded.includes(octEvent));
    Assert.assertTrue("Should include newer event",
        unbounded.includes(novEvent));

    // test range limiting
    Assert.assertNotNull("from should succeed",
        unbounded.from("timestamp", now));
    Assert.assertNotNull("fromAfter should succeed",
        unbounded.fromAfter("timestamp", now));
    Assert.assertNotNull("to should succeed",
        unbounded.to("timestamp", now));
    Assert.assertNotNull("toBefore should succeed",
        unbounded.toBefore("timestamp", now));
    Assert.assertNotNull("with should succeed",
        unbounded.with("timestamp", now));
  }

  @Test
  public void testNotPartitioned() throws Exception {
    final DatasetDescriptor flatDescriptor = new DatasetDescriptor
        .Builder(testDescriptor)
        .partitionStrategy((PartitionStrategy) null)
        .build();
    final Dataset<StandardEvent> notPartitioned =
        repo.create("ns", "flat", flatDescriptor);

    // test events
    Assert.assertTrue("Should include any StandardEvent",
        notPartitioned.includes(event));
    Assert.assertTrue("Should include even null events",
        notPartitioned.includes(null));
    Assert.assertTrue("Should include older event",
        notPartitioned.includes(sepEvent));
    Assert.assertTrue("Should include event",
        notPartitioned.includes(octEvent));
    Assert.assertTrue("Should include newer event",
        notPartitioned.includes(novEvent));

    // test range limiting
    Assert.assertNotNull("from should succeed",
        notPartitioned.from("timestamp", now));
    Assert.assertNotNull("fromAfter should succeed",
        notPartitioned.fromAfter("timestamp", now));
    Assert.assertNotNull("to should succeed",
        notPartitioned.to("timestamp", now));
    Assert.assertNotNull("toBefore should succeed",
        notPartitioned.toBefore("timestamp", now));
    Assert.assertNotNull("with should succeed",
        notPartitioned.with("timestamp", now));
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void testReadySignalWithConstraint() {
    Assume.assumeTrue(unbounded instanceof Signalable);
    final long instant = new DateTime(2013, 10, 1, 0, 0, DateTimeZone.UTC).getMillis();
    final Signalable withSpecificTimestamp =
        (Signalable) unbounded.with("timestamp", instant);

    Assert.assertFalse("Should not be ready initially", withSpecificTimestamp.isReady());
    withSpecificTimestamp.signalReady();
    Assert.assertTrue("Should be ready after signal", withSpecificTimestamp.isReady());

    Signalable multipleWithView = (Signalable)unbounded.with("timestamp", instant+1, instant+2);

    Assert.assertFalse("Should not be ready initially", multipleWithView.isReady());
    multipleWithView.signalReady();
    Assert.assertTrue("Should be ready after signal", multipleWithView.isReady());
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void testReadySignalFromConstraint() {
    Assume.assumeTrue(unbounded instanceof Signalable);
    final long instant = new DateTime(2013, 10, 1, 0, 0, DateTimeZone.UTC).getMillis();
    final Signalable fromSpecificTimestamp =
        (Signalable) unbounded.from("timestamp", instant);

    Assert.assertFalse("Should not be ready initially", fromSpecificTimestamp.isReady());
    fromSpecificTimestamp.signalReady();
    Assert.assertTrue("Should be ready after signal", fromSpecificTimestamp.isReady());
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void testReadySignalFromAfterConstraint() {
    Assume.assumeTrue(unbounded instanceof Signalable);
    final long instant = new DateTime(2013, 10, 1, 0, 0, DateTimeZone.UTC).getMillis();
    final Signalable fromAfterSpecificTimestamp =
        (Signalable) unbounded.from("timestamp", instant);

    Assert.assertFalse("Should not be ready initially", fromAfterSpecificTimestamp.isReady());
    fromAfterSpecificTimestamp.signalReady();
    Assert.assertTrue("Should be ready after signal", fromAfterSpecificTimestamp.isReady());
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void testReadySignalToConstraint() {
    Assume.assumeTrue(unbounded instanceof Signalable);
    final long instant = new DateTime(2013, 10, 1, 0, 0, DateTimeZone.UTC).getMillis();
    final Signalable toSpecificTimestamp =
        (Signalable) unbounded.to("timestamp", instant);

    Assert.assertFalse("Should not be ready initially", toSpecificTimestamp.isReady());
    toSpecificTimestamp.signalReady();
    Assert.assertTrue("Should be ready after signal", toSpecificTimestamp.isReady());
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void testReadySignalToBeforeConstraint() {
    Assume.assumeTrue(unbounded instanceof Signalable);
    final long instant = new DateTime(2013, 10, 1, 0, 0, DateTimeZone.UTC).getMillis();
    final Signalable toBeforeSpecificTimestamp =
        (Signalable) unbounded.toBefore("timestamp", instant);

    Assert.assertFalse("Should not be ready initially", toBeforeSpecificTimestamp.isReady());
    toBeforeSpecificTimestamp.signalReady();
    Assert.assertTrue("Should be ready after signal", toBeforeSpecificTimestamp.isReady());
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void testReadySignalMultipleConstraints() {
    Assume.assumeTrue(unbounded instanceof Signalable);
    final long instant = new DateTime(2013, 10, 1, 0, 0, DateTimeZone.UTC).getMillis();
    final Signalable withTwoConstrainedFields =
        (Signalable) unbounded.with("timestamp", instant).with("user_id", 0L);

    Assert.assertFalse("Should not be ready initially", withTwoConstrainedFields.isReady());
    withTwoConstrainedFields.signalReady();
    Assert.assertTrue("Should be ready after signal", withTwoConstrainedFields.isReady());
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void testReadySignalsAroundComparableBoundaries() {
    Assume.assumeTrue(unbounded instanceof Signalable);
    final long instant = new DateTime(2013, 10, 1, 0, 0, DateTimeZone.UTC).getMillis();
    final Signalable withSpecificTimestamp =
        (Signalable) unbounded.with("timestamp", instant);

    Assert.assertFalse("Base view should not be ready initially",
        ((Signalable)unbounded).isReady());

    Assert.assertFalse("Should not be ready initially",
        withSpecificTimestamp.isReady());
    withSpecificTimestamp.signalReady();
    Assert.assertTrue("Should be ready after signal",
        withSpecificTimestamp.isReady());

    final Signalable beforeSpecificTimestamp =
        (Signalable) unbounded.toBefore("timestamp", instant);
    Assert.assertFalse("To before the instant should not be ready",
        beforeSpecificTimestamp.isReady());

    final Signalable afterSpecificTimestamp =
        (Signalable) unbounded.fromAfter("timestamp", instant);
    Assert.assertFalse("To after the instant should not be ready",
        afterSpecificTimestamp.isReady());

    final Signalable immediatelyAfterSpecificTimestamp =
        (Signalable) unbounded.with("timestamp", instant+1);
    Assert.assertFalse("After the instant should not be ready",
        immediatelyAfterSpecificTimestamp.isReady());

    final Signalable immediatelyBeforeSpecificTimestamp =
        (Signalable) unbounded.with("timestamp", instant-1);
    Assert.assertFalse("Before the instant should not be ready",
        immediatelyBeforeSpecificTimestamp.isReady());

    final Signalable includingSpecificTimestamp =
        (Signalable) unbounded.from("timestamp", instant-1).to("timestamp", instant+1);
    Assert.assertFalse("Including the instant should not be ready",
        includingSpecificTimestamp.isReady());
  }

}
