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

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.data.DatasetReader;
import com.cloudera.data.filesystem.FileSystemDatasetReader;
import com.google.common.io.Resources;

public class TestFileSystemDatasetReader {

  private FileSystem fileSystem;
  private Schema testSchema;

  @Before
  public void setUp() throws IOException {
    fileSystem = FileSystem.get(new Configuration());
    testSchema = new Schema.Parser().parse(Resources.getResource("schema/string.avsc")
        .openStream());
  }

  @Test
  public void testRead() throws IOException {
    DatasetReader<String> reader = null;
    int records = 0;

    try {
      reader = new FileSystemDatasetReader<String>(fileSystem, new Path(Resources
          .getResource("data/strings-100.avro").getFile()), testSchema);

      reader.open();

      while (reader.hasNext()) {
        String record = reader.read();

        Assert.assertNotNull(record);
        records++;
      }
    } finally {
      if (reader != null) {
        reader.close();
      }
    }

    Assert.assertEquals(100, records);
  }

  @Test(expected = IllegalStateException.class)
  public void testHasNextOnNonOpenWriterFails() throws IOException {
    DatasetReader<String> reader = null;
    try {
      reader = new FileSystemDatasetReader<String>(fileSystem, new Path(Resources
          .getResource("data/strings-100.avro").getFile()), testSchema);
      reader.hasNext();
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

}
