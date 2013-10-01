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
package com.cloudera.cdk.data.filesystem;

import com.cloudera.cdk.data.Dataset;
import com.cloudera.cdk.data.DatasetReader;
import com.cloudera.cdk.data.DatasetWriter;
import com.cloudera.cdk.data.PartitionKey;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Assert;

public class DatasetTestUtilities {

  public final static Schema STRING_SCHEMA = loadSchema("schema/string.avsc");
  public final static Schema USER_SCHEMA = loadSchema("schema/user.avsc");
  public final static URI USER_SCHEMA_URL = findSchemaURI("schema/user.avsc");

  private static Schema loadSchema(String resource) {
    try {
      return new Schema.Parser().parse(Resources.getResource(
          resource).openStream());
    } catch (IOException e) {
      throw new IllegalStateException("Cannot load " + resource);
    }
  }

  private static URI findSchemaURI(String resource) {
    try {
      return Resources.getResource(resource).toURI();
    } catch (URISyntaxException e) {
      throw new IllegalStateException("Cannot load " + resource);
    }
  }

  public static void writeTestUsers(Dataset ds, int count) {
    writeTestUsers(ds, count, 0);
  }

  public static void writeTestUsers(Dataset ds, int count, int start) {
    DatasetWriter<GenericData.Record> writer = null;
    try {
      writer = ds.newWriter();
      writer.open();
      for (int i = start; i < count + start; i++) {
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
    final Set<String> usernames = Sets.newHashSet();
    for (int i = 0; i < count; i++) {
      usernames.add("test-" + i);
    }

    checkReaderBehavior(ds.<GenericData.Record>newReader(), count,
        new RecordValidator<GenericData.Record>() {
      @Override
      public void validate(GenericData.Record record, int recordNum) {
        Assert.assertTrue(usernames.remove((String) record.get("username")));
        Assert.assertNotNull(record.get("email"));
      }
    });

    Assert.assertTrue(usernames.isEmpty());
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
      reader = ds.newReader();
      reader.open();
      for (GenericData.Record record : reader) {
        records.add(record);
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

  public static interface RecordValidator<R> {
    void validate(R record, int recordNum);
  }

  public static <R> void checkReaderBehavior(
      DatasetReader<R> reader, int totalRecords, RecordValidator<R> validator) {
    Assert.assertFalse("Reader is open before open()", reader.isOpen());

    try {
      reader.open();

      Assert.assertTrue("Reader is not open after open()", reader.isOpen());

      checkReaderIteration(reader, totalRecords, validator);

    } finally {
      reader.close();
    }

    Assert.assertFalse("Reader is open after close()", reader.isOpen());
  }

  public static <R> void checkReaderIteration(DatasetReader<R> reader,
      int expectedRecordCount, RecordValidator<R> validator) {
    int recordCount = 0;

    Assert.assertTrue("Reader is not open", reader.isOpen());
    Assert.assertTrue("Reader has no records, expected " + expectedRecordCount,
        (expectedRecordCount == 0) || reader.hasNext());

    for (R record : reader) {
      // add calls to hasNext, which should not affect the iteration
      reader.hasNext();
      Assert.assertNotNull(record);
      validator.validate(record, recordCount);
      recordCount++;
    }

    Assert.assertFalse("Reader is empty, but hasNext is true",
        reader.hasNext());

    // verify that NoSuchElementException is thrown when hasNext returns false
    try {
      reader.next();
      Assert.fail("Reader did not throw NoSuchElementException");
    } catch (NoSuchElementException ex) {
      // this is the correct behavior
    }

    Assert.assertTrue("Reader is empty, but should be open", reader.isOpen());

    // verify the correct number of records were produced
    // if hasNext advances the reader, then this will be wrong
    Assert.assertEquals("Incorrect number of records",
        expectedRecordCount, recordCount);
  }

}
