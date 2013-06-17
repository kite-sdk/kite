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

import com.cloudera.data.DatasetDescriptor;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import java.io.IOException;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.cloudera.data.filesystem.DatasetTestUtilities.STRING_SCHEMA;

public class TestMultiFileDatasetReader {

  private FileSystem fileSystem;

  @Before
  public void setUp() throws IOException {
    fileSystem = FileSystem.get(new Configuration());
  }

  @Test
  public void test() throws IOException {
    Path testFile = new Path(Resources.getResource("data/strings-100.avro")
        .getFile());

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder().schema(STRING_SCHEMA).get();
    MultiFileDatasetReader<Record> reader = new MultiFileDatasetReader<Record>(
        fileSystem, Lists.newArrayList(testFile, testFile), descriptor);

    int records = 0;

    try {
      reader.open();

      while (reader.hasNext()) {
        Record record = reader.read();
        Assert.assertNotNull(record);
        Assert.assertEquals(String.valueOf(records % 100), record.get("text"));
        records++;
      }
    } finally {
      reader.close();
    }

    Assert.assertEquals(200, records);
  }

}
