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

import java.io.IOException;
import java.util.concurrent.Callable;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.Formats;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.TestHelpers;

public class TestAvroAppender extends MiniDFSTest {
  private static final Schema schema = Schema.create(Schema.Type.STRING);

  @Test
  public void testAvroSyncDFS() throws Exception {
    String auth = getDFS().getUri().getAuthority();
    final FileSystem fs = getDFS();
    final Path path = new Path("hdfs://" + auth + "/tmp/test.avro");
    AvroAppender<String> appender = new AvroAppender<String>(
        fs, path, schema, Formats.AVRO.getDefaultCompressionType());

    appender.open();
    for (int i = 0; i < 10; i += 1) {
      String s = "string-" + i;
      appender.append(s);
    }

    TestHelpers.assertThrows("Should not be able to read file, nothing written",
        DatasetIOException.class, new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            count(fs, path);
            return null;
          }
        });

    appender.flush();
    appender.sync();

    // after sync, the first 10 records should be readable
    Assert.assertEquals("Should find the first 10 records",
        10, count(fs, path));

    for (int i = 10; i < 20; i += 1) {
      String s = "string-" + i;
      appender.append(s);
    }

    // the records should still be buffered
    Assert.assertEquals("Newly written records should still be buffered",
        10, count(fs, path));

    appender.close();
    appender.cleanup();

    // the records should still be buffered
    Assert.assertEquals("All records should be found after close",
        20, count(fs, path));
  }

  @Test
  @Ignore(value="LocalFileSystem is broken!?")
  public void testAvroSyncLocalFS() throws Exception {
    final FileSystem fs = getFS();
    final Path path = new Path("file:/tmp/test.avro");
    AvroAppender<String> appender = new AvroAppender<String>(
        fs, path, schema, Formats.AVRO.getDefaultCompressionType());

    appender.open();
    for (int i = 0; i < 10; i += 1) {
      String s = "string-" + i;
      appender.append(s);
    }

    TestHelpers.assertThrows("Should not be able to read file, nothing written",
        DatasetIOException.class, new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            count(fs, path);
            return null;
          }
        });

    appender.flush();
    appender.sync();

    // after sync, the first 10 records should be readable
    Assert.assertEquals("Should find the first 10 records",
        10, count(fs, path));

    for (int i = 10; i < 20; i += 1) {
      String s = "string-" + i;
      appender.append(s);
    }

    // the records should still be buffered
    Assert.assertEquals("Newly written records should still be buffered",
        10, count(fs, path));

    appender.close();
    appender.cleanup();

    // the records should still be buffered
    Assert.assertEquals("All records should be found after close",
        20, count(fs, path));
  }

  public int count(FileSystem fs, Path path) {
    FileSystemDatasetReader<String> reader =
        new FileSystemDatasetReader<String>(fs, path, schema, String.class);
    int count = 0;
    reader.initialize();
    for (String s : reader) {
      count += 1;
      System.err.println(s);
    }
    reader.close();
    return count;
  }
}
