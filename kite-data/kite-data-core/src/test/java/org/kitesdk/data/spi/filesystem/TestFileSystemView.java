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

import org.kitesdk.data.RefinableView;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.TrashPolicy;
import org.kitesdk.data.Signalable;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import java.util.Iterator;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.PartitionView;
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.LastModifiedAccessor;
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
  @SuppressWarnings("unchecked")
  public void testCoveringPartitions() throws IOException {
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

    // get the covering partitions with a reliable order
    Map<String, PartitionView<StandardEvent>> partitions = Maps.newTreeMap();
    for (PartitionView<StandardEvent> view : unbounded.getCoveringPartitions()) {
      partitions.put(view.getLocation().toString(), view);
    }
    Iterator<PartitionView<StandardEvent>> iter = partitions.values().iterator();

    assertTrue(iter.hasNext());
    View v1 = iter.next();
    assertTrue(v1.includes(standardEvent(sepEvent.getTimestamp())));
    assertFalse(v1.includes(standardEvent(octEvent.getTimestamp())));
    assertFalse(v1.includes(standardEvent(novEvent.getTimestamp())));

    assertTrue(iter.hasNext());
    View v2 = iter.next();
    assertFalse(v2.includes(standardEvent(sepEvent.getTimestamp())));
    assertTrue(v2.includes(standardEvent(octEvent.getTimestamp())));
    assertFalse(v2.includes(standardEvent(novEvent.getTimestamp())));

    assertTrue(iter.hasNext());
    View v3 = iter.next();
    assertFalse(v3.includes(standardEvent(sepEvent.getTimestamp())));
    assertFalse(v3.includes(standardEvent(octEvent.getTimestamp())));
    assertTrue(v3.includes(standardEvent(novEvent.getTimestamp())));

    assertFalse(iter.hasNext());
  }

  private StandardEvent standardEvent(long timestamp) {
    return StandardEvent.newBuilder(event).setTimestamp(timestamp).build();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDelete() throws Exception {
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

    final Path root = new Path("target/data/ns/test");
    final Path y2013 = new Path("target/data/ns/test/year=2013");
    final Path sep = new Path("target/data/ns/test/year=2013/month=09");
    final Path sep12 = new Path("target/data/ns/test/year=2013/month=09/day=12");
    final Path oct = new Path("target/data/ns/test/year=2013/month=10");
    final Path oct12 = new Path("target/data/ns/test/year=2013/month=10/day=12");
    final Path nov = new Path("target/data/ns/test/year=2013/month=11");
    final Path nov11 = new Path("target/data/ns/test/year=2013/month=11/day=11");
    assertDirectoriesExist(fs, root, y2013, sep, sep12, oct, oct12, nov, nov11);

    long julStart = new DateTime(2013, 6, 1, 0, 0, DateTimeZone.UTC).getMillis();
    long sepStart = new DateTime(2013, 9, 1, 0, 0, DateTimeZone.UTC).getMillis();
    final long nov11Start = new DateTime(2013, 11, 11, 0, 0, DateTimeZone.UTC).getMillis();
    final long nov12Start = new DateTime(2013, 11, 12, 0, 0, DateTimeZone.UTC).getMillis();
    long decStart = new DateTime(2013, 12, 1, 0, 0, DateTimeZone.UTC).getMillis();
    final long sepInstant = sepEvent.getTimestamp();
    final long sep12End = new DateTime(2013, 9, 13, 0, 0, DateTimeZone.UTC).getMillis() - 1;
    final long octInstant = octEvent.getTimestamp();
    // aligned with second boundaries, but not partition boundaries
    final long oct12Start = new DateTime(2013, 10, 12, 0, 0, 0, DateTimeZone.UTC).getMillis();
    final long oct12BadStart = new DateTime(2013, 10, 12, 0, 1, 0, DateTimeZone.UTC).getMillis();
    final long oct12End = new DateTime(2013, 10, 12, 23, 59, 59, 999, DateTimeZone.UTC).getMillis();
    final long oct12BadEnd = new DateTime(2013, 10, 12, 23, 59, 57, 999, DateTimeZone.UTC).getMillis();

    Assert.assertFalse("Delete should return false to indicate no changes",
        unbounded.from("timestamp", decStart).deleteAll());
    Assert.assertFalse("Delete should return false to indicate no changes",
        unbounded.from("timestamp", julStart).toBefore("timestamp", sepStart).deleteAll());

    // cannot delete mid-partition
    TestHelpers.assertThrows(
        "Delete should fail if not aligned with partition boundary",
        UnsupportedOperationException.class, new Runnable() {
      @Override
      public void run() {
        unbounded.to("timestamp", sepInstant).deleteAll();
      }
    });
    TestHelpers.assertThrows(
        "Delete should fail if not aligned with partition boundary",
        UnsupportedOperationException.class, new Runnable() {
      @Override
      public void run() {
        unbounded.toBefore("timestamp", sep12End).deleteAll();
      }
    });

    // delete everything up through September
    assertTrue("Delete should return true to indicate FS changed",
        unbounded.to("timestamp", sep12End).deleteAll());
    assertDirectoriesDoNotExist(fs, sep12, sep);
    assertDirectoriesExist(fs, root, y2013, oct, oct12, nov, nov11);
    Assert.assertFalse("Delete should return false to indicate no changes",
        unbounded.to("timestamp", sep12End).deleteAll());
    assertDirectoriesDoNotExist(fs, sep12, sep);
    assertDirectoriesExist(fs, root, y2013, oct, oct12, nov, nov11);

    // cannot delete mid-partition
    TestHelpers.assertThrows(
        "Delete should fail if not aligned with partition boundary",
        UnsupportedOperationException.class, new Runnable() {
      @Override
      public void run() {
        unbounded.from("timestamp", nov11Start).to("timestamp", nov12Start).deleteAll();
      }
    });
    TestHelpers.assertThrows(
        "Delete should fail if not aligned with partition boundary",
        UnsupportedOperationException.class, new Runnable() {
      @Override
      public void run() {
        unbounded.fromAfter("timestamp", nov11Start).toBefore("timestamp", nov12Start).deleteAll();
      }
    });

    // delete November 11 and later
    assertTrue("Delete should return true to indicate FS changed",
        unbounded.from("timestamp", nov11Start).toBefore("timestamp", nov12Start).deleteAll());
    assertDirectoriesDoNotExist(fs, sep12, sep, nov11, nov);
    assertDirectoriesExist(fs, root, y2013, oct, oct12);
    Assert.assertFalse("Delete should return false to indicate no changes",
        unbounded.from("timestamp", nov11Start).toBefore("timestamp", nov12Start).deleteAll());
    assertDirectoriesDoNotExist(fs, sep12, sep, nov11, nov);
    assertDirectoriesExist(fs, root, y2013, oct, oct12);

    // cannot delete mid-partition
    TestHelpers.assertThrows(
        "Delete should fail if not aligned with partition boundary",
        UnsupportedOperationException.class, new Runnable() {
      @Override
      public void run() {
        unbounded.from("timestamp", octInstant).to("timestamp", octInstant).deleteAll();
      }
    });
    TestHelpers.assertThrows(
        "Delete should fail if not aligned with partition boundary",
        UnsupportedOperationException.class, new Runnable() {
      @Override
      public void run() {
        unbounded.from("timestamp", oct12BadStart).to("timestamp", oct12End).deleteAll();
      }
    });
    TestHelpers.assertThrows(
        "Delete should fail if not aligned with partition boundary",
        UnsupportedOperationException.class, new Runnable() {
      @Override
      public void run() {
        unbounded.from("timestamp", oct12Start).to("timestamp", oct12BadEnd).deleteAll();
      }
    });

    // delete October and the 2013 directory
    assertTrue("Delete should return true to indicate FS changed",
        unbounded.from("timestamp", oct12Start).to("timestamp", oct12End).deleteAll());
    assertDirectoriesDoNotExist(fs, y2013, sep12, sep, oct12, oct, nov11, nov);
    assertDirectoriesExist(fs, root);
    Assert.assertFalse("Delete should return false to indicate no changes",
        unbounded.from("timestamp", oct12Start).to("timestamp", oct12End).deleteAll());
    assertDirectoriesDoNotExist(fs, y2013, sep12, sep, oct12, oct, nov11, nov);
    assertDirectoriesExist(fs, root);

    Assert.assertFalse("Delete should return false to indicate no changes",
        unbounded.deleteAll());
    assertDirectoriesDoNotExist(fs, y2013, sep12, sep, oct12, oct, nov11, nov);
    assertDirectoriesExist(fs, root);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMoveToTrash() throws Exception {
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

    final Path root = new Path("target/data/ns/test");
    final Path y2013 = new Path("target/data/ns/test/year=2013");
    final Path sep = new Path("target/data/ns/test/year=2013/month=09");
    final Path sep12 = new Path("target/data/ns/test/year=2013/month=09/day=12");
    final Path oct = new Path("target/data/ns/test/year=2013/month=10");
    final Path oct12 = new Path("target/data/ns/test/year=2013/month=10/day=12");
    final Path nov = new Path("target/data/ns/test/year=2013/month=11");
    final Path nov11 = new Path("target/data/ns/test/year=2013/month=11/day=11");
    assertDirectoriesExist(fs, root, y2013, sep, sep12, oct, oct12, nov, nov11);

    long julStart = new DateTime(2013, 6, 1, 0, 0, DateTimeZone.UTC).getMillis();
    long sepStart = new DateTime(2013, 9, 1, 0, 0, DateTimeZone.UTC).getMillis();
    final long nov11Start = new DateTime(2013, 11, 11, 0, 0, DateTimeZone.UTC).getMillis();
    final long nov12Start = new DateTime(2013, 11, 12, 0, 0, DateTimeZone.UTC).getMillis();
    long decStart = new DateTime(2013, 12, 1, 0, 0, DateTimeZone.UTC).getMillis();
    final long sepInstant = sepEvent.getTimestamp();
    final long sep12End = new DateTime(2013, 9, 13, 0, 0, DateTimeZone.UTC).getMillis() - 1;
    final long octInstant = octEvent.getTimestamp();
    // aligned with second boundaries, but not partition boundaries
    final long oct12Start = new DateTime(2013, 10, 12, 0, 0, 0, DateTimeZone.UTC).getMillis();
    final long oct12BadStart = new DateTime(2013, 10, 12, 0, 1, 0, DateTimeZone.UTC).getMillis();
    final long oct12End = new DateTime(2013, 10, 12, 23, 59, 59, 999, DateTimeZone.UTC).getMillis();
    final long oct12BadEnd = new DateTime(2013, 10, 12, 23, 59, 57, 999, DateTimeZone.UTC).getMillis();

    Assert.assertFalse("Delete should return false to indicate no changes",
        unbounded.from("timestamp", decStart).moveToTrash());
    Assert.assertFalse("Delete should return false to indicate no changes",
        unbounded.from("timestamp", julStart).toBefore("timestamp", sepStart).moveToTrash());

    // cannot delete mid-partition
    TestHelpers.assertThrows(
        "Delete should fail if not aligned with partition boundary",
        UnsupportedOperationException.class, new Runnable() {
          @Override
          public void run() {
            unbounded.to("timestamp", sepInstant).deleteAll();
          }
        });
    TestHelpers.assertThrows(
        "Delete should fail if not aligned with partition boundary",
        UnsupportedOperationException.class, new Runnable() {
          @Override
          public void run() {
            unbounded.toBefore("timestamp", sep12End).moveToTrash();
          }
        });

    // delete everything up through September
    assertTrue("Delete should return true to indicate FS changed",
        unbounded.to("timestamp", sep12End).moveToTrash());
    assertDirectoriesDoNotExist(fs, sep12, sep);
    assertDirectoriesInTrash(fs, trashPolicy, sep12, sep);
    assertDirectoriesExist(fs, root, y2013, oct, oct12, nov, nov11);
    Assert.assertFalse("Delete should return false to indicate no changes",
        unbounded.to("timestamp", sep12End).moveToTrash());
    assertDirectoriesDoNotExist(fs, sep12, sep);
    assertDirectoriesExist(fs, root, y2013, oct, oct12, nov, nov11);

    // cannot delete mid-partition
    TestHelpers.assertThrows(
        "Delete should fail if not aligned with partition boundary",
        UnsupportedOperationException.class, new Runnable() {
          @Override
          public void run() {
            unbounded.from("timestamp", nov11Start).to("timestamp", nov12Start).moveToTrash();
          }
        });
    TestHelpers.assertThrows(
        "Delete should fail if not aligned with partition boundary",
        UnsupportedOperationException.class, new Runnable() {
          @Override
          public void run() {
            unbounded.fromAfter("timestamp", nov11Start).toBefore("timestamp", nov12Start).moveToTrash();
          }
        });

    // delete November 11 and later
    assertTrue("Delete should return true to indicate FS changed",
        unbounded.from("timestamp", nov11Start).toBefore("timestamp", nov12Start).moveToTrash());
    assertDirectoriesDoNotExist(fs, sep12, sep, nov11, nov);
    assertDirectoriesExist(fs, root, y2013, oct, oct12);
    Assert.assertFalse("Delete should return false to indicate no changes",
        unbounded.from("timestamp", nov11Start).toBefore("timestamp", nov12Start).moveToTrash());
    assertDirectoriesDoNotExist(fs, sep12, sep, nov11, nov);
    assertDirectoriesInTrash(fs, trashPolicy, sep12, sep, nov11, nov);
    assertDirectoriesExist(fs, root, y2013, oct, oct12);

    // cannot delete mid-partition
    TestHelpers.assertThrows(
        "Delete should fail if not aligned with partition boundary",
        UnsupportedOperationException.class, new Runnable() {
          @Override
          public void run() {
            unbounded.from("timestamp", octInstant).to("timestamp", octInstant).moveToTrash();
          }
        });
    TestHelpers.assertThrows(
        "Delete should fail if not aligned with partition boundary",
        UnsupportedOperationException.class, new Runnable() {
          @Override
          public void run() {
            unbounded.from("timestamp", oct12BadStart).to("timestamp", oct12End).moveToTrash();
          }
        });
    TestHelpers.assertThrows(
        "Delete should fail if not aligned with partition boundary",
        UnsupportedOperationException.class, new Runnable() {
          @Override
          public void run() {
            unbounded.from("timestamp", oct12Start).to("timestamp", oct12BadEnd).moveToTrash();
          }
        });

    // delete October and the 2013 directory
    assertTrue("Delete should return true to indicate FS changed",
        unbounded.from("timestamp", oct12Start).to("timestamp", oct12End).moveToTrash());
    assertDirectoriesDoNotExist(fs, y2013, sep12, sep, oct12, oct, nov11, nov);
    assertDirectoriesInTrash(fs, trashPolicy, y2013, sep12, sep, oct12, oct, nov11, nov);
    assertDirectoriesExist(fs, root);
    Assert.assertFalse("Delete should return false to indicate no changes",
        unbounded.from("timestamp", oct12Start).to("timestamp", oct12End).moveToTrash());
    assertDirectoriesDoNotExist(fs, y2013, sep12, sep, oct12, oct, nov11, nov);
    assertDirectoriesInTrash(fs, trashPolicy, y2013, sep12, sep, oct12, oct, nov11, nov);
    assertDirectoriesExist(fs, root);

    Assert.assertFalse("Delete should return false to indicate no changes",
        unbounded.moveToTrash());
    assertDirectoriesDoNotExist(fs, y2013, sep12, sep, oct12, oct, nov11, nov);
    assertDirectoriesInTrash(fs, trashPolicy, y2013, sep12, sep, oct12, nov11, nov);
    assertDirectoriesExist(fs, root);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testUnboundedDelete() throws Exception {
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

    final Path root = new Path("target/data/ns/test");
    final Path y2013 = new Path("target/data/ns/test/year=2013");
    final Path sep = new Path("target/data/ns/test/year=2013/month=09");
    final Path sep12 = new Path("target/data/ns/test/year=2013/month=09/day=12");
    final Path oct = new Path("target/data/ns/test/year=2013/month=10");
    final Path oct12 = new Path("target/data/ns/test/year=2013/month=10/day=12");
    final Path nov = new Path("target/data/ns/test/year=2013/month=11");
    final Path nov11 = new Path("target/data/ns/test/year=2013/month=11/day=11");
    assertDirectoriesExist(fs, root, y2013, sep, sep12, oct, oct12, nov, nov11);

    Assert.assertTrue("Delete should return true to indicate data was deleted.",
        unbounded.deleteAll());
    assertDirectoriesDoNotExist(fs, y2013, sep12, sep, oct12, oct, nov11, nov);
    assertDirectoriesExist(fs, root);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testUnboundedMoveToTrash() throws Exception {
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

    final Path root = new Path("target/data/ns/test");
    final Path y2013 = new Path("target/data/ns/test/year=2013");
    final Path sep = new Path("target/data/ns/test/year=2013/month=09");
    final Path sep12 = new Path("target/data/ns/test/year=2013/month=09/day=12");
    final Path oct = new Path("target/data/ns/test/year=2013/month=10");
    final Path oct12 = new Path("target/data/ns/test/year=2013/month=10/day=12");
    final Path nov = new Path("target/data/ns/test/year=2013/month=11");
    final Path nov11 = new Path("target/data/ns/test/year=2013/month=11/day=11");
    assertDirectoriesExist(fs, root, y2013, sep, sep12, oct, oct12, nov, nov11);

    Assert.assertTrue("Delete should return true to indicate data was deleted.",
        unbounded.moveToTrash());
    assertDirectoriesDoNotExist(fs, y2013, sep12, sep, oct12, oct, nov11, nov);
    assertDirectoriesExist(fs, root);
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void testSignalUpdatesLastModified() {
    long lastModified = ((LastModifiedAccessor)unbounded).getLastModified();

    long signaledTime = -1;
    long spinStart = System.currentTimeMillis();
    while(lastModified >= signaledTime && (System.currentTimeMillis() - spinStart) <= 2000) {
      ((Signalable)unbounded).signalReady();
      signaledTime = ((LastModifiedAccessor)unbounded).getLastModified();
    }
    Assert.assertTrue("Signaling should update last modified time", signaledTime > lastModified);
  }

  @Test
  public void testNullSignalManager() {
    FileSystemDataset<StandardEvent> ds =
        (FileSystemDataset<StandardEvent>) unbounded.getDataset();
    FileSystemView<StandardEvent> view =
        new FileSystemView<StandardEvent>(ds, null, null, StandardEvent.class);

    // getlast modified
    Assert.assertTrue("Last modified does not require access to signal manager",
        view.getLastModified() >= -1);

    view.signalReady();
    Assert.assertFalse("View should not be signaled without manager", view.isReady());
  }

  @SuppressWarnings("deprecation")
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

  @Test
  public void testRangeWithEmptyDirectory() throws Exception {
    long start = new DateTime(2013, 9, 1, 0, 0, DateTimeZone.UTC).getMillis();
    long end = new DateTime(2013, 11, 14, 0, 0, DateTimeZone.UTC).getMillis();
    final RefinableView<StandardEvent> range = unbounded
            .from("timestamp", start).toBefore("timestamp", end);

    DatasetWriter<StandardEvent> writer = null;
    try {
      writer = range.newWriter();
      writer.write(sepEvent);
      writer.write(novEvent);
    } finally {
      Closeables.close(writer, false);
    }

    // All expected datasets and partitions are present
    final Path root = new Path("target/data/ns/test");
    final Path y2013 = new Path("target/data/ns/test/year=2013");
    final Path sep = new Path("target/data/ns/test/year=2013/month=09");
    final Path sep12 = new Path("target/data/ns/test/year=2013/month=09/day=12");
    final Path nov = new Path("target/data/ns/test/year=2013/month=11");
    final Path nov11 = new Path("target/data/ns/test/year=2013/month=11/day=11");
    assertDirectoriesExist(fs, root, y2013, sep, sep12, nov, nov11);

    // Recreate an empty directory that represents a valid partition for october 12.
    final Path oct = new Path("target/data/ns/test/year=2013/month=10");
    final Path oct12 = new Path("target/data/ns/test/year=2013/month=10/day=12");
    assertTrue("mkdir should return true to indicate FS changed.", fs.mkdirs(oct12));
    assertDirectoriesExist(fs, oct, oct12);

    // Test reading from September thru November. The range includes the empty directory.
    assertContentEquals(Sets.newHashSet(sepEvent, novEvent), range);

  }
  
  public static void assertDirectoriesInTrash(FileSystem fs, TrashPolicy trashPolicy, Path... dirs)
          throws IOException {
    Path trashDir = trashPolicy.getCurrentTrashDir();

    for (Path path : dirs) {
      Path trashPath = Path.mergePaths(trashDir, fs.makeQualified(path));
      assertTrue("Directory should exist in trash: " + trashPath, fs.exists(trashPath));
    }
  }
}
