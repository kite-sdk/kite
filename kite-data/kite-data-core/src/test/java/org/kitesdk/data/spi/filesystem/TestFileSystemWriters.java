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

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.spi.InitializeAccessor;
import org.kitesdk.data.spi.ReaderWriterState;

public abstract class TestFileSystemWriters extends MiniDFSTest {

  public static final Schema TEST_SCHEMA = SchemaBuilder.record("Message").fields()
      .requiredLong("id")
      .requiredString("message")
      .nullableString("name","")
      .endRecord();

  public abstract FileSystemWriter<Record> newWriter(Path directory, Schema datasetSchema, Schema writerSchema);
  public abstract DatasetReader<Record> newReader(Path path, Schema schema);

  protected FileSystem fs = null;
  protected Path testDirectory = null;
  protected FileSystemWriter<Record> fsWriter = null;

  @Before
  public void setup() throws IOException {
    this.fs = getDFS();
    this.testDirectory = new Path(Files.createTempDir().getAbsolutePath());
    this.fsWriter = newWriter(testDirectory, TEST_SCHEMA, TEST_SCHEMA);
  }

  @After
  public void tearDown() throws IOException {
    fs.delete(testDirectory, true);
  }

  @Test
  public void testBasicWrite() throws IOException {
    init(fsWriter);

    if (fsWriter.useTempPath) {
      FileStatus[] stats = fs.listStatus(testDirectory, PathFilters.notHidden());
      Assert.assertEquals("Should contain no visible files", 0, stats.length);
      stats = fs.listStatus(testDirectory);
      Assert.assertEquals("Should contain a hidden file", 1, stats.length);
    } else {
      FileStatus[] stats = fs.listStatus(testDirectory, hidden());
      Assert.assertEquals("Should contain no hidden files", 0, stats.length);
      stats = fs.listStatus(testDirectory, PathFilters.notHidden());
      Assert.assertEquals("Should contain a visible file", 1, stats.length);
    }

    List<Record> written = Lists.newArrayList();
    for (long i = 0; i < 10000; i += 1) {
      Record record = record(i, "test-" + i);
      fsWriter.write(record);
      written.add(record);
    }

    if (fsWriter.useTempPath) {
      FileStatus[] stats = fs.listStatus(testDirectory, PathFilters.notHidden());
      Assert.assertEquals("Should contain no visible files", 0, stats.length);
      stats = fs.listStatus(testDirectory);
      Assert.assertEquals("Should contain a hidden file", 1, stats.length);
    } else {
      FileStatus[] stats = fs.listStatus(testDirectory, hidden());
      Assert.assertEquals("Should contain no hidden files", 0, stats.length);
      stats = fs.listStatus(testDirectory, PathFilters.notHidden());
      Assert.assertEquals("Should contain a visible file", 1, stats.length);
    }

    fsWriter.close();

    FileStatus[] stats = fs.listStatus(testDirectory, hidden());
    Assert.assertEquals("Should contain no hidden files", 0, stats.length);
    stats = fs.listStatus(testDirectory, PathFilters.notHidden());
    Assert.assertEquals("Should contain a visible data file", 1, stats.length);

    DatasetReader<Record> reader = newReader(stats[0].getPath(), TEST_SCHEMA);
    Assert.assertEquals("Should match written records",
        written, Lists.newArrayList((Iterator) init(reader)));
  }
  
  @Test
  public void testWriteWithOldSchema() throws IOException {
    
    Schema writerSchema = SchemaBuilder.record("Message").fields()
        .requiredLong("id")
        .requiredString("message")
        .endRecord();

    fsWriter = newWriter(testDirectory, TEST_SCHEMA, writerSchema);
    init(fsWriter);

    for (long i = 0; i < 1000; i += 1) {
    
    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(TEST_SCHEMA)
        .set("id", i).set("message","test-"+ i);
      fsWriter.write(recordBuilder.build());
    }

    fsWriter.close();

    final FileStatus[]   stats = fs.listStatus(testDirectory, PathFilters.notHidden());
    Assert.assertEquals("Should match with writer schema",
        writerSchema, FileSystemUtil.schema("record", fs, stats[0].getPath()));
  }

  @Test
  public void testTargetFileSize() throws IOException {
    init(fsWriter);

    Path rolledFilePath = null;
    if (fsWriter.useTempPath) {
      FileStatus[] stats = fs.listStatus(testDirectory, PathFilters.notHidden());
      Assert.assertEquals("Should contain no visible files", 0, stats.length);
      stats = fs.listStatus(testDirectory);
      Assert.assertEquals("Should contain a hidden file", 1, stats.length);
    } else {
      FileStatus[] stats = fs.listStatus(testDirectory, hidden());
      Assert.assertEquals("Should contain no hidden files", 0, stats.length);
      stats = fs.listStatus(testDirectory, PathFilters.notHidden());
      Assert.assertEquals("Should contain a visible file", 1, stats.length);
      rolledFilePath = stats[0].getPath();
    }

    List<Record> written = Lists.newArrayList();
    for (long i = 0; i < 100000; i += 1) {
      // use a UUID to make the file size bigger
      Record record = record(i, UUID.randomUUID().toString());
      fsWriter.write(record);
      written.add(record);
    }

    FileStatus rolledFile = null;
    if (fsWriter.useTempPath) {
      FileStatus[] stats = fs.listStatus(testDirectory, PathFilters.notHidden());
      Assert.assertEquals("Should contain a rolled file", 1, stats.length);

      // keep track of the rolled file for the size check
      rolledFile = stats[0];

      stats = fs.listStatus(testDirectory);
      Assert.assertEquals("Should contain a hidden file and a rolled file",
          2, stats.length);
    } else {
      FileStatus[] stats = fs.listStatus(testDirectory, PathFilters.notHidden());
      Assert.assertEquals("Should contain a new file and a rolled file", 2, stats.length);

      // keep track of the rolled file for the size check
      for (FileStatus stat : stats) {
        if (stat.getPath().equals(rolledFilePath)) {
          rolledFile = stat;
        }
      }
    }

    fsWriter.close();

    FileStatus[] stats = fs.listStatus(testDirectory, PathFilters.notHidden());
    Assert.assertEquals("Should contain two visible data files", 2, stats.length);

    List<Record> actualContent = Lists.newArrayList();
    DatasetReader<Record> reader = newReader(stats[0].getPath(), TEST_SCHEMA);
    Iterators.addAll(actualContent, init(reader));
    reader = newReader(stats[1].getPath(), TEST_SCHEMA);
    Iterators.addAll(actualContent, init(reader));

    Assert.assertEquals("Should have the same number of records",
        written.size(), actualContent.size());
    Assert.assertTrue("Should match written records",
        Sets.newHashSet(written).equals(Sets.newHashSet(actualContent)));

    double ratioToTarget = (((double) rolledFile.getLen()) / 2 / 1024 / 1024);
    Assert.assertTrue(
        "Should be within 10% of target size: " + ratioToTarget * 100,
        ratioToTarget > 0.90 && ratioToTarget < 1.10);
  }

  @Test
  public void testTimeBasedRoll() throws Exception {
    // time-based operations are done when clock ticks are passed to the writer
    // with ClockReady#tick.
    init(fsWriter);

    Path rolledFilePath = null;
    if (fsWriter.useTempPath) {
      FileStatus[] stats = fs.listStatus(testDirectory, PathFilters.notHidden());
      Assert.assertEquals("Should contain no visible files", 0, stats.length);
      stats = fs.listStatus(testDirectory);
      Assert.assertEquals("Should contain a hidden file", 1, stats.length);
    } else {
      FileStatus[] stats = fs.listStatus(testDirectory, hidden());
      Assert.assertEquals("Should contain no hidden files", 0, stats.length);
      stats = fs.listStatus(testDirectory, PathFilters.notHidden());
      Assert.assertEquals("Should contain a visible file", 1, stats.length);
      rolledFilePath = stats[0].getPath();
    }

    // write the first half of the records
    List<Record> firstHalf = Lists.newArrayList();
    for (long i = 0; i < 50000; i += 1) {
      // use a UUID to make the file size bigger
      Record record = record(i, UUID.randomUUID().toString());
      fsWriter.write(record);
      firstHalf.add(record);
    }

    // the writer is configured to roll every 1 second, so this guarantees it
    // will roll when tick is called
    Thread.sleep(1000);
    fsWriter.tick(); // send a clock signal to trigger the roll

    // write the second half of the records
    List<Record> secondHalf = Lists.newArrayList();
    for (long i = 0; i < 50000; i += 1) {
      // use a UUID to make the file size bigger
      Record record = record(i, UUID.randomUUID().toString());
      fsWriter.write(record);
      secondHalf.add(record);
    }

    FileStatus rolledFile = null;
    if (fsWriter.useTempPath) {
      FileStatus[] stats = fs.listStatus(testDirectory, PathFilters.notHidden());
      Assert.assertEquals("Should contain a rolled file", 1, stats.length);

      // keep track of the rolled file for the content check
      rolledFile = stats[0];

      stats = fs.listStatus(testDirectory);
      Assert.assertEquals("Should contain a hidden file and a rolled file",
          2, stats.length);
    } else {
      FileStatus[] stats = fs.listStatus(testDirectory, PathFilters.notHidden());
      Assert.assertEquals("Should contain a new file and a rolled file", 2, stats.length);

      // keep track of the rolled file for the size check
      for (FileStatus stat : stats) {
        if (stat.getPath().equals(rolledFilePath)) {
          rolledFile = stat;
        }
      }
    }

    fsWriter.close();

    FileStatus[] stats = fs.listStatus(testDirectory, PathFilters.notHidden());
    Assert.assertEquals("Should contain two visible data files", 2, stats.length);

    DatasetReader<Record> reader = newReader(rolledFile.getPath(), TEST_SCHEMA);
    int count = 0;
    for (Record actual : init(reader)) {
      count += 1;
      Assert.assertEquals(firstHalf.get(((Long) actual.get("id")).intValue()), actual);
    }
    Assert.assertEquals("First half should have 50,000 records", 50000, count);

    FileStatus closedFile = rolledFile.equals(stats[0]) ? stats[1] : stats[0];
    reader = newReader(closedFile.getPath(), TEST_SCHEMA);
    count = 0;
    for (Record actual : init(reader)) {
      count += 1;
      Assert.assertEquals(secondHalf.get(((Long) actual.get("id")).intValue()), actual);
    }
    Assert.assertEquals("Second half should have 50,000 records", 50000, count);
  }

  @Test
  public void testDiscardEmptyFiles() throws IOException {
    init(fsWriter);
    fsWriter.close();

    FileStatus[] stats = fs.listStatus(testDirectory, hidden());
    Assert.assertEquals("Should not contain any hidden files", 0, stats.length);

    stats = fs.listStatus(testDirectory, PathFilters.notHidden());
    Assert.assertEquals("Should not contain any visible files", 0, stats.length);
  }

  @Test
  public void testDiscardErrorFiles() throws IOException {
    init(fsWriter);
    for (long i = 0; i < 10000; i += 1) {
      fsWriter.write(record(i, "test-" + i));
    }

    // put the writer into an error state, simulating either:
    // 1. A failed record with an IOException or unknown RuntimeException
    // 2. A failed flush or sync for IncrementableWriters
    fsWriter.state = ReaderWriterState.ERROR;

    fsWriter.close();

    FileStatus[] stats = fs.listStatus(testDirectory, hidden());
    Assert.assertEquals("Should not contain any hidden files", 0, stats.length);

    stats = fs.listStatus(testDirectory, PathFilters.notHidden());
    Assert.assertEquals("Should not contain any visible files", 0, stats.length);
  }

  protected static Record record(long id, String message) {
    Record record = new Record(TEST_SCHEMA);
    record.put("id", id);
    record.put("message", message);
    return record;
  }

  protected static <C> C init(C obj) {
    if (obj instanceof InitializeAccessor) {
      ((InitializeAccessor) obj).initialize();
    }
    return obj;
  }

  protected static PathFilter hidden() {
    final PathFilter notHidden = PathFilters.notHidden();
    return new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return !notHidden.accept(path);
      }
    };
  }
}
