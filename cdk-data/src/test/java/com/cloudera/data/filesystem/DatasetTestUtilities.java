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

import com.cloudera.data.Dataset;
import com.cloudera.data.DatasetReader;
import com.cloudera.data.DatasetWriter;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import java.io.IOException;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Assert;

public class DatasetTestUtilities {

  public final static Schema STRING_SCHEMA = loadSchema("schema/string.avsc");
  public final static Schema USER_SCHEMA = loadSchema("schema/user.avsc");

  private static Schema loadSchema(String resource) {
    try {
      return new Schema.Parser().parse(Resources.getResource(
          resource).openStream());
    } catch (IOException e) {
      throw new IllegalStateException("Cannot load " + resource);
    }
  }

  public static void writeTestUsers(Dataset ds, int count) {
    DatasetWriter<GenericData.Record> writer = null;
    try {
      writer = ds.getWriter();
      writer.open();
      for (int i = 0; i < count; i++) {
        GenericData.Record record = new GenericRecordBuilder(USER_SCHEMA)
            .set("username", "test-" + i)
            .set("email", "email-" + i).build();
        writer.write(record);
      }
      writer.flush();
    } finally {
      if (writer != null) {
        writer.close();
      }
    }
  }

  public static void checkTestUsers(Dataset ds, int count) {
    DatasetReader<GenericData.Record> reader = null;
    try {
      reader = ds.getReader();
      reader.open();

      // record order is not guaranteed, so check that we have read all the
      // records
      Set<String> usernames = Sets.newHashSet();
      for (int i = 0; i < count; i++) {
        usernames.add("test-" + i);
      }
      for (int i = 0; i < count; i++) {
        Assert.assertTrue(reader.hasNext());
        GenericData.Record actualRecord = reader.read();
        Assert.assertTrue(usernames.remove((String) actualRecord
            .get("username")));
        Assert.assertNotNull(actualRecord.get("email"));
      }
      Assert.assertTrue(usernames.isEmpty());
      Assert.assertFalse(reader.hasNext());
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  public static int datasetSize(Dataset ds) {
    int size = 0;
    DatasetReader<GenericData.Record> reader = null;
    try {
      reader = ds.getReader();
      reader.open();
      while (reader.hasNext()) {
        reader.read();
        size++;
      }
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
    return size;
  }
}
