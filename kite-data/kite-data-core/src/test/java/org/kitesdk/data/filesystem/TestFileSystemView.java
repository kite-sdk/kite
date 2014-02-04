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

package org.kitesdk.data.filesystem;

import java.util.Iterator;
import org.joda.time.DateTime;
import org.junit.Ignore;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.TestRefinableViews;
import org.kitesdk.data.event.StandardEvent;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestFileSystemView extends TestRefinableViews {

  public TestFileSystemView(boolean distributed) {
    super(distributed);
  }

  @Override
  public DatasetRepository newRepo() {
    return new FileSystemDatasetRepository.Builder()
        .configuration(conf)
        .rootDirectory(URI.create("target/data"))
        .build();
  }

  @After
  public void removeDataPath() throws IOException {
    fs.delete(new Path("target/data"), true);
  }

  @Test
  @Ignore("getCoveringPartitions is not yet implemented")
  @SuppressWarnings("unchecked")
  public void testCoveringPartitions() {
    // NOTE: this is an un-restricted write so all should succeed
    final DatasetWriter<StandardEvent> writer = unbounded.newWriter();
    try {
      writer.open();
      writer.write(sepEvent);
      writer.write(octEvent);
      writer.write(novEvent);
    } finally {
      writer.close();
    }

    Iterator<View<StandardEvent>> coveringPartitions =
        ((FileSystemView) unbounded).getCoveringPartitions().iterator();

    assertTrue(coveringPartitions.hasNext());
    View v1 = coveringPartitions.next();
    assertTrue(v1.includes(standardEvent(sepEvent.getTimestamp())));
    assertFalse(v1.includes(standardEvent(octEvent.getTimestamp())));
    assertFalse(v1.includes(standardEvent(novEvent.getTimestamp())));

    assertTrue(coveringPartitions.hasNext());
    View v2 = coveringPartitions.next();
    assertFalse(v2.includes(standardEvent(sepEvent.getTimestamp())));
    assertTrue(v2.includes(standardEvent(octEvent.getTimestamp())));
    assertFalse(v2.includes(standardEvent(novEvent.getTimestamp())));

    assertTrue(coveringPartitions.hasNext());
    View v3 = coveringPartitions.next();
    assertFalse(v3.includes(standardEvent(sepEvent.getTimestamp())));
    assertFalse(v3.includes(standardEvent(octEvent.getTimestamp())));
    assertTrue(v3.includes(standardEvent(novEvent.getTimestamp())));

    assertFalse(coveringPartitions.hasNext());
  }

  private StandardEvent standardEvent(long timestamp) {
    return StandardEvent.newBuilder(event).setTimestamp(timestamp).build();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDelete() throws Exception {
    // NOTE: this is an un-restricted write so all should succeed
    final DatasetWriter<StandardEvent> writer = unbounded.newWriter();
    try {
      writer.open();
      writer.write(sepEvent);
      writer.write(octEvent);
      writer.write(novEvent);
    } finally {
      writer.close();
    }

    final Path root = new Path("target/data/test");
    final Path y2013 = new Path("target/data/test/year=2013");
    final Path sep = new Path("target/data/test/year=2013/month=09");
    final Path sep12 = new Path("target/data/test/year=2013/month=09/day=12");
    final Path oct = new Path("target/data/test/year=2013/month=10");
    final Path oct12 = new Path("target/data/test/year=2013/month=10/day=12");
    final Path nov = new Path("target/data/test/year=2013/month=11");
    final Path nov11 = new Path("target/data/test/year=2013/month=11/day=11");
    assertDirectoriesExist(fs, root, y2013, sep, sep12, oct, oct12, nov, nov11);

    long julStart = new DateTime(2013, 6, 1, 0, 0).getMillis();
    long sepStart = new DateTime(2013, 9, 1, 0, 0).getMillis();
    long nov11Start = new DateTime(2013, 11, 11, 0, 0).getMillis();
    long nov12Start = new DateTime(2013, 11, 12, 0, 0).getMillis();
    long decStart = new DateTime(2013, 12, 1, 0, 0).getMillis();
    long sepInstant = sepEvent.getTimestamp();
    long octInstant = octEvent.getTimestamp();

    Assert.assertFalse("Delete should return false to indicate no changes",
        deleteAll(unbounded.from("timestamp", julStart).toBefore("timestamp", sepStart)));
    Assert.assertFalse("Delete should return false to indicate no changes",
        deleteAll(unbounded.from("timestamp", decStart)));

    // delete everything up to September
    assertTrue("Delete should return true to indicate FS changed",
        deleteAll(unbounded.to("timestamp", sepInstant)));
    assertDirectoriesDoNotExist(fs, sep12, sep);
    assertDirectoriesExist(fs, root, y2013, oct, oct12, nov, nov11);
    Assert.assertFalse("Delete should return false to indicate no changes",
        deleteAll(unbounded.to("timestamp", sepInstant)));

    // delete November 11 and later
    assertTrue("Delete should return true to indicate FS changed",
        deleteAll(unbounded.from("timestamp", nov11Start).to("timestamp", nov12Start)));
    assertDirectoriesDoNotExist(fs, sep12, sep, nov11, nov);
    assertDirectoriesExist(fs, root, y2013, oct, oct12);
    Assert.assertFalse("Delete should return false to indicate no changes",
        deleteAll(unbounded.from("timestamp", nov11Start).to("timestamp", nov12Start)));

    // delete October and the 2013 directory
    assertTrue("Delete should return true to indicate FS changed",
        deleteAll(unbounded.from("timestamp", octInstant).to("timestamp", octInstant)));
    assertDirectoriesDoNotExist(fs, y2013, sep12, sep, oct12, oct, nov11, nov);
    assertDirectoriesExist(fs, root);
    Assert.assertFalse("Delete should return false to indicate no changes",
        deleteAll(unbounded.from("timestamp", octInstant).to("timestamp", octInstant)));

    Assert.assertFalse("Delete should return false to indicate no changes",
        ((FileSystemDataset) unbounded).deleteAll());
  }

  private boolean deleteAll(View v) {
    return ((FileSystemView) v).deleteAll(); // until deleteAll is exposed on View
  }

  public static void assertDirectoriesExist(FileSystem fs, Path... dirs)
      throws IOException {
    for (Path path : dirs) {
      assertTrue("Directory should exist: " + path,
          fs.exists(path) && fs.isDirectory(path));
    }
  }

  public static void assertDirectoriesDoNotExist(FileSystem fs, Path... dirs)
      throws IOException {
    for (Path path : dirs) {
      assertTrue("Directory should not exist: " + path,
          !fs.exists(path));
    }
  }
}
