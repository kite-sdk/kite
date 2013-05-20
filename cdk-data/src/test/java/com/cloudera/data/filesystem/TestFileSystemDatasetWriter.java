/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.data.filesystem;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFileSystemDatasetWriter {

  private File testDirectory;
  private FileSystem fileSystem;

  @Before
  public void setUp() throws IOException {
    testDirectory = Files.createTempDir();
    fileSystem = FileSystem.get(new Configuration());
  }

  @After
  public void tearDown() throws IOException {
    fileSystem.delete(new Path(testDirectory.getAbsolutePath()), true);
  }

  @Test
  public void testWrite() throws IOException {
    FileSystemDatasetWriter<String> writer = new FileSystemDatasetWriter<String>(
        fileSystem, new Path(testDirectory.getAbsolutePath(), "write-1.avro"),
        Schema.create(Type.STRING), true);

    writer.open();

    for (int i = 0; i < 100; i++) {
      writer.write("entry " + i);

      if (i % 10 == 0) {
        writer.flush();
      }
    }

    writer.close();
  }

}
