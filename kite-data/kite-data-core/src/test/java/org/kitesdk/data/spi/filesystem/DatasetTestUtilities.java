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
package org.kitesdk.data.spi.filesystem;

import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.spi.PartitionKey;
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
import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.InitializeAccessor;
import org.kitesdk.data.spi.PartitionedDataset;

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

  public static void writeTestUsers(Dataset<GenericData.Record> ds, int count) {
    writeTestUsers(ds, count, 0);
  }

  public static void writeTestUsers(Dataset<GenericData.Record> ds, int count, int start) {
    writeTestUsers(ds, count, start, "email");
  }

  public static void writeTestUsers(Dataset<GenericData.Record> ds, int count, int start, String... fields) {
    DatasetWriter<GenericData.Record> writer = null;
    try {
      writer = ds.newWriter();
      for (int i = start; i < count + start; i++) {
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(ds.getDescriptor
            ().getSchema()).set("username", "test-" + i);
        for (String field : fields) {
          recordBuilder.set(field, field + "-" + i);
        }
        writer.write(recordBuilder.build());
      }
      writer.flush();
    } finally {
      if (writer != null) {
        writer.close();
      }
    }
  }

  public static void checkTestUsers(Dataset<GenericData.Record> ds, int count) {
    checkTestUsers(ds, count, "email");
  }

  public static void checkTestUsers(Dataset<GenericData.Record> ds, int count, int start) {
    checkTestUsers(ds, count, start, "email");
  }

  public static void checkTestUsers(Dataset<GenericData.Record> ds, int count, final String... fields) {
    checkTestUsers(ds, count, 0, fields);
  }

  public static void checkTestUsers(Dataset<GenericData.Record> ds, int count, int start, final String... fields) {
    final Set<String> usernames = Sets.newHashSet();
    for (int i = start; i < count + start; i++) {
      usernames.add("test-" + i);
    }

    checkReaderBehavior(ds.newReader(), count,
        new RecordValidator<GenericData.Record>() {
          @Override
          public void validate(GenericData.Record record, int recordNum) {
            String username = record.get("username").toString();
            Assert.assertTrue("Username not found: " + username, usernames.remove(username));
            for (String field : fields) {
              Assert.assertNotNull("Field is null: " + field + ", record: " + record,
                  record.get(field));
            }
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
      Assert.assertTrue(usernames.remove(actualRecord
          .get("username").toString()));
      Assert.assertNotNull(actualRecord.get("email"));
    }
    Assert.assertTrue(usernames.isEmpty());
  }

  public static <E> Set<E> materialize(View<E> ds) {
    Set<E> records = Sets.newHashSet();
    DatasetReader<E> reader = null;
    try {
      reader = ds.newReader();
      for (E record : reader) {
        records.add(record);
      }
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
    return records;
  }

  public static <E> int datasetSize(View<E> ds) {
    return materialize(ds).size();
  }

  @SuppressWarnings("deprecation")
  public static <E> void testPartitionKeysAreEqual(PartitionedDataset<E> ds,
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
    // this is now used for both initialized and not initialized records because
    // initialization now happens automatically in newReader
    if (!reader.isOpen() && reader instanceof InitializeAccessor) {
      ((InitializeAccessor) reader).initialize();
    }

    try {
      Assert.assertTrue("Reader should be open", reader.isOpen());

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
