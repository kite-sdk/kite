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
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Flushable;
import org.kitesdk.data.Syncable;
import org.kitesdk.data.spi.ReaderWriterState;

public class TestAvroWriter extends TestFileSystemWriters {
  @Override
  public FileSystemWriter<Record> newWriter(Path directory, Schema schema) {
    return FileSystemWriter.newWriter(fs, directory,
        new DatasetDescriptor.Builder()
            .schema(schema)
            .format("avro")
            .build());
  }

  @Override
  public DatasetReader<Record> newReader(Path path, Schema schema) {
    return new FileSystemDatasetReader<Record>(fs, path, schema, Record.class);
  }

  @Test
  public void testIsFlushable() {
    Assert.assertTrue(fsWriter instanceof Flushable);
  }

  @Test
  public void testIsSyncable() {
    Assert.assertTrue(fsWriter instanceof Syncable);
  }

  @Test
  public void testCommitFlushedRecords() throws IOException {
    init(fsWriter);

    List<Record> written = Lists.newArrayList();
    long i;
    for (i = 0; i < 10000; i += 1) {
      Record record = record(i, "test-" + i);
      fsWriter.write(record);
      written.add(record);
    }

    ((Flushable) fsWriter).flush();

    for (i = 10000; i < 11000; i += 1) {
      fsWriter.write(record(i, "test-" + i));
    }

    // put the writer into an error state, simulating either:
    // 1. A failed record with an IOException or unknown RuntimeException
    // 2. A failed flush or sync for IncrementableWriters
    fsWriter.state = ReaderWriterState.ERROR;

    fsWriter.close();

    FileStatus[] stats = fs.listStatus(testDirectory, PathFilters.notHidden());
    Assert.assertEquals("Should contain a visible data file", 1, stats.length);

    DatasetReader<Record> reader = newReader(stats[0].getPath(), TEST_SCHEMA);
    Assert.assertEquals("Should match written records",
        written, Lists.newArrayList((Iterator) init(reader)));
  }
}
