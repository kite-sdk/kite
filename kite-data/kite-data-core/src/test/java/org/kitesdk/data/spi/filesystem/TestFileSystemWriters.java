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

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Formats;
import org.kitesdk.data.spi.InitializeAccessor;

public abstract class TestFileSystemWriters<E> {

  public abstract DatasetWriter<E> newWriter(Path directory);

  protected FileSystem fs = null;
  protected Path testDirectory = null;
  protected DatasetWriter<E> fsWriter = null;

  @Before
  public void setup() throws IOException {
    this.fs = FileSystem.getLocal(new Configuration());
    this.testDirectory = new Path(Files.createTempDir().getAbsolutePath());
    this.fsWriter = newWriter(testDirectory);
  }

  @After
  public void tearDown() throws IOException {
    fs.delete(testDirectory, true);
  }

  @Test
  public void testDiscardEmptyFiles() throws IOException {
    init(fsWriter);
    fsWriter.close();
    Assert.assertEquals("Should not contain any files", 0,
        ImmutableList.copyOf(fs.listStatus(testDirectory)).size());
  }

  @Test
  public void testWrite() throws IOException {
    AvroAppender<String> writer = new AvroAppender<String>(
        fs, new Path(testDirectory, "write-1.avro"),
        Schema.create(Schema.Type.STRING), Formats.AVRO.getDefaultCompressionType());

    writer.open();

    for (int i = 0; i < 100; i++) {
      writer.append("entry " + i);

      if (i % 10 == 0) {
        writer.flush();
      }
    }

    writer.close();
  }

  public void init(DatasetWriter<?> writer) {
    if (writer instanceof InitializeAccessor) {
      ((InitializeAccessor) writer).initialize();
    }
  }
}
