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
import com.cloudera.data.PartitionKey;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Assert;

public class DatasetTestUtilities {

  public final static Schema STRING_SCHEMA = loadSchema("schema/string.avsc");
  public final static Schema USER_SCHEMA = loadSchema("schema/user.avsc");
  public final static URL USER_SCHEMA_URL = Resources.getResource("schema/user.avsc");

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
    checkTestUsers(materialize(ds), count);
  }

  public static void checkTestUsers(Set<GenericData.Record> records, int count) {
    Assert.assertEquals("Wrong number of records", count, records.size());
    // record order is not guaranteed, so check that we have read all the
    // records
    Set<String> usernames = Sets.newHashSet();
    for (int i = 0; i < count; i++) {
      usernames.add("test-" + i);
    }
    for (GenericData.Record actualRecord : records) {
      Assert.assertTrue(usernames.remove((String) actualRecord
          .get("username")));
      Assert.assertNotNull(actualRecord.get("email"));
    }
    Assert.assertTrue(usernames.isEmpty());
  }

  public static Set<GenericData.Record> materialize(Dataset ds) {
    Set<GenericData.Record> records = Sets.newHashSet();
    DatasetReader<GenericData.Record> reader = null;
    try {
      reader = ds.getReader();
      reader.open();
      while (reader.hasNext()) {
        records.add(reader.read());
      }
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
    return records;
  }

  public static int datasetSize(Dataset ds) {
    return materialize(ds).size();
  }

  public static void testPartitionKeysAreEqual(Dataset ds,
      PartitionKey... expectedKeys) {
    Set<PartitionKey> expected = Sets.newHashSet(expectedKeys);
    Set<PartitionKey> actual = Sets.newHashSet(Iterables.transform(ds.getPartitions(),
        new Function<Dataset, PartitionKey>() {
      @Override
      public PartitionKey apply(Dataset input) {
        return ((FileSystemDataset) input).getPartitionKey();
      }
    }));
    Assert.assertEquals(expected, actual);
  }
}
