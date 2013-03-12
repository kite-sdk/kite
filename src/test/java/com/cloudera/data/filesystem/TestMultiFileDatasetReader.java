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

import com.cloudera.data.filesystem.MultiFileDatasetReader;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;

public class TestMultiFileDatasetReader {

  private FileSystem fileSystem;
  private Schema testSchema;

  @Before
  public void setUp() throws IOException {
    fileSystem = FileSystem.get(new Configuration());
    testSchema = new Schema.Parser().parse(Resources.getResource("schema/string.avsc")
        .openStream());
  }

  @Test
  public void test() throws IOException {
    Path testFile = new Path(Resources.getResource("data/strings-100.avro")
        .getFile());

    MultiFileDatasetReader<String> reader = new MultiFileDatasetReader<String>(
        fileSystem, Lists.newArrayList(testFile, testFile), testSchema);

    int records = 0;

    try {
      reader.open();

      while (reader.hasNext()) {
        String record = reader.read();
        Assert.assertNotNull(record);
        Assert.assertEquals("test-" + records % 100, record);
        records++;
      }
    } finally {
      reader.close();
    }

    Assert.assertEquals(200, records);
  }

}
