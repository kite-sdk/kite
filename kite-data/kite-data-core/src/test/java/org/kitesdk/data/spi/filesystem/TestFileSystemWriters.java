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

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
      .endRecord();

  public abstract FileSystemWriter<Record> newWriter(Path directory, Schema schema);
  public abstract DatasetReader<Record> newReader(Path path, Schema schema);

  protected FileSystem fs = null;
  protected Path testDirectory = null;
  protected FileSystemWriter<Record> fsWriter = null;

  @Before
  public void setup() throws IOException {
    this.fs = getDFS();
    this.testDirectory = new Path(Files.createTempDir().getAbsolutePath());
    this.fsWriter = newWriter(testDirectory, TEST_SCHEMA);
  }

  @After
  public void tearDown() throws IOException {
    fs.delete(testDirectory, true);
  }

  @Test
  public void testBasicWrite() throws IOException {
    init(fsWriter);

    FileStatus[] stats = fs.listStatus(testDirectory, PathFilters.notHidden());
    Assert.assertEquals("Should contain no visible files", 0, stats.length);
    stats = fs.listStatus(testDirectory);
    Assert.assertEquals("Should contain a hidden file", 1, stats.length);

    List<Record> written = Lists.newArrayList();
    for (long i = 0; i < 10000; i += 1) {
      Record record = record(i, "test-" + i);
      fsWriter.write(record);
      written.add(record);
    }

    stats = fs.listStatus(testDirectory, PathFilters.notHidden());
    Assert.assertEquals("Should contain no visible files", 0, stats.length);
    stats = fs.listStatus(testDirectory);
    Assert.assertEquals("Should contain a hidden file", 1, stats.length);

    fsWriter.close();

    stats = fs.listStatus(testDirectory, PathFilters.notHidden());
    Assert.assertEquals("Should contain a visible data file", 1, stats.length);

    DatasetReader<Record> reader = newReader(stats[0].getPath(), TEST_SCHEMA);
    Assert.assertEquals("Should match written records",
        written, Lists.newArrayList((Iterator) init(reader)));
  }

  @Test
  public void testDiscardEmptyFiles() throws IOException {
    init(fsWriter);
    fsWriter.close();

    FileStatus[] stats = fs.listStatus(testDirectory, PathFilters.notHidden());
    Assert.assertEquals("Should not contain any files", 0, stats.length);
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

    FileStatus[] stats = fs.listStatus(testDirectory, PathFilters.notHidden());
    Assert.assertEquals("Should not contain any files", 0, stats.length);
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
}
