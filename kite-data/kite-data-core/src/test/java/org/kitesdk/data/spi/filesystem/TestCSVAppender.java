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

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.spi.DataModelUtil;

public class TestCSVAppender extends MiniDFSTest {
  private static final Schema schema = SchemaBuilder
      .record("User").fields()
      .requiredInt("id")
      .requiredString("email")
      .endRecord();

  @Test
  public void testCSVSyncDFS() throws Exception {
    String auth = getDFS().getUri().getAuthority();
    final FileSystem fs = getDFS();
    final Path path = new Path("hdfs://" + auth + "/tmp/test.csv");
    final DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(schema)
        .build();
    CSVAppender<GenericRecord> appender = new CSVAppender<GenericRecord>(fs, path, descriptor);
    GenericRecord record = new GenericData.Record(schema);

    appender.open();
    for (int i = 0; i < 10; i += 1) {
      record.put("id", i);
      record.put("email", Integer.toString(i) + "@example.com");
      appender.append(record);
    }

    Assert.assertEquals("Should not find records before flush",
        0, count(fs, path, descriptor));

    appender.flush();
    appender.sync();

    // after sync, the first 10 records should be readable
    Assert.assertEquals("Should find the first 10 records",
        10, count(fs, path, descriptor));

    for (int i = 10; i < 20; i += 1) {
      record.put("id", i);
      record.put("email", Integer.toString(i) + "@example.com");
      appender.append(record);
    }

    // the records should still be buffered
    Assert.assertEquals("Newly written records should still be buffered",
        10, count(fs, path, descriptor));

    appender.close();
    appender.cleanup();

    // the records should still be buffered
    Assert.assertEquals("All records should be found after close",
        20, count(fs, path, descriptor));
  }

  @Test
  @Ignore(value="LocalFileSystem is broken!?")
  public void testCSVSyncLocalFS() throws Exception {
    final FileSystem fs = getFS();
    final Path path = new Path("file:/tmp/test.csv");
    final DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(schema)
        .build();
    CSVAppender<GenericRecord> appender = new CSVAppender<GenericRecord>(fs, path, descriptor);
    GenericRecord record = new GenericData.Record(schema);

    appender.open();
    for (int i = 0; i < 10; i += 1) {
      record.put("id", i);
      record.put("email", Integer.toString(i) + "@example.com");
      appender.append(record);
    }

    Assert.assertEquals("Should not find records before flush",
        0, count(fs, path, descriptor));

    appender.flush();
    appender.sync();

    // after sync, the first 10 records should be readable
    Assert.assertEquals("Should find the first 10 records",
        10, count(fs, path, descriptor));

    for (int i = 10; i < 20; i += 1) {
      record.put("id", i);
      record.put("email", Integer.toString(i) + "@example.com");
      appender.append(record);
    }

    // the records should still be buffered
    Assert.assertEquals("Newly written records should still be buffered",
        10, count(fs, path, descriptor));

    appender.close();
    appender.cleanup();

    // the records should still be buffered
    Assert.assertEquals("All records should be found after close",
        20, count(fs, path, descriptor));
  }

  public int count(FileSystem fs, Path path, DatasetDescriptor descriptor) {
    CSVFileReader<GenericRecord> reader = new CSVFileReader<GenericRecord>(
        fs, path, descriptor,
        DataModelUtil.accessor(GenericRecord.class, descriptor.getSchema()));
    int count = 0;
    reader.initialize();
    for (GenericRecord r : reader) {
      count += 1;
      System.err.println(r);
    }
    reader.close();
    return count;
  }

}
