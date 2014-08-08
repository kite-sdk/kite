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

package org.kitesdk.data.spi.filesystem;

import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import java.io.IOException;
import java.net.URI;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.View;
import org.kitesdk.data.event.StandardEvent;
import org.kitesdk.data.spi.DatasetRepository;

public class TestSimpleView {

  protected static final StandardEvent event = StandardEvent.newBuilder()
      .setEventInitiator("TestSimpleView")
      .setEventName("TestEvent")
      .setUserId(0)
      .setSessionId("session-0")
      .setIp("localhost")
      .setTimestamp(System.currentTimeMillis() + 35405168L)
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

  // from subclasses
  protected DatasetRepository repo = null;

  public DatasetRepository newRepo() {
    return new FileSystemDatasetRepository.Builder()
        .configuration(conf)
        .rootDirectory(URI.create("target/data"))
        .build();
  }

  protected Configuration conf = null;
  protected FileSystem fs;
  protected PartitionStrategy strategy = null;
  protected DatasetDescriptor testDescriptor = null;
  protected Dataset<StandardEvent> testDataset = null;

  @Before
  public void setup() throws Exception {
    this.conf = new Configuration();
    this.fs = FileSystem.get(conf);

    this.repo = newRepo();
    this.strategy = new PartitionStrategy.Builder()
        .year("timestamp")
        .month("timestamp")
        .day("timestamp")
        .hash("user_id", 2)
        .build();
    this.testDescriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:standard_event.avsc")
        .partitionStrategy(strategy)
        .build();
    this.testDataset = repo.create("ns", "test", testDescriptor);
  }

  @After
  public void removeDataPath() throws IOException {
    fs.delete(new Path("target/data"), true);
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

  @Test
  public void testLimitedReader() throws IOException {
    // NOTE: this is an un-restricted write so all should succeed
    DatasetWriter<StandardEvent> writer = null;
    try {
      writer = testDataset.newWriter();
      writer.write(sepEvent);
      writer.write(octEvent);
      writer.write(novEvent);
    } finally {
      Closeables.close(writer, false);
    }

    // unbounded
    assertContentEquals(Sets.newHashSet(sepEvent, octEvent, novEvent),
        testDataset);

    long sepEnd = new DateTime(2013, 9, 30, 12, 59, 59, 999, DateTimeZone.UTC).getMillis();
    long octInstant = octEvent.getTimestamp();
    long novStart = new DateTime(2013, 11, 1, 0, 0, DateTimeZone.UTC).getMillis();

    // single bound
    assertContentEquals(Sets.newHashSet(octEvent, novEvent),
        testDataset.from("timestamp", octInstant));
    assertContentEquals(Sets.newHashSet(octEvent, novEvent),
        testDataset.fromAfter("timestamp", sepEnd));
    assertContentEquals(Sets.newHashSet(sepEvent, octEvent),
        testDataset.to("timestamp", octInstant));
    assertContentEquals(Sets.newHashSet(sepEvent, octEvent),
        testDataset.toBefore("timestamp", novStart));

    // double bound
    assertContentEquals(Sets.newHashSet(octEvent),
        testDataset.from("timestamp", octInstant).toBefore("timestamp", novStart));

    // with
    assertContentEquals(Sets.newHashSet(sepEvent, novEvent),
        testDataset.with("user_id", 0L));
    assertContentEquals(Sets.newHashSet(sepEvent),
        testDataset.with("user_id", 0L).to("timestamp", octInstant));
    assertContentEquals(Sets.newHashSet(sepEvent),
        testDataset.to("timestamp", octInstant).with("user_id", 0L));

  }

  @Test
  public void testRefineIdentity() throws Exception {
      PartitionStrategy strategy = new PartitionStrategy.Builder()
              .identity("user_id")
              .build();

      DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
              .schemaUri("resource:standard_event.avsc")
              .partitionStrategy(strategy)
              .build();

      // Create a separate dataset to avoid conflicts with the above.
      Dataset<StandardEvent> identityDataset = repo.create(
          "ns", "test_identity", descriptor);

      DatasetWriter<StandardEvent> writer = null;

      try {
          writer = identityDataset.newWriter();
          writer.write(sepEvent);
          writer.write(octEvent);
          writer.write(novEvent);
      } finally {
          Closeables.close(writer, false);
      }

      assertContentEquals(Sets.newHashSet(sepEvent, novEvent),
              identityDataset.with("user_id", 0L));
  }
}
