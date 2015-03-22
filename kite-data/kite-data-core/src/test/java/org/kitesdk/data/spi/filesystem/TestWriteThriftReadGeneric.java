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

import com.google.common.io.Files;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.LocalFileSystem;
import org.kitesdk.data.TestDatasetReaders;
import org.kitesdk.data.event.thrift.Value;
import org.kitesdk.data.spi.filesystem.DatasetTestUtilities.RecordValidator;

public class TestWriteThriftReadGeneric extends TestDatasetReaders<GenericRecord> {

  private static final int totalRecords = 100;
  protected static FileSystem fs = null;
  protected static Path testDirectory = null;
  protected static Dataset<GenericRecord> readerDataset;

  @BeforeClass
  public static void setup() throws IOException {
    fs = LocalFileSystem.getInstance();
    testDirectory = new Path(Files.createTempDir().getAbsolutePath());

    DatasetDescriptor desc = new DatasetDescriptor.Builder()
        .schema(Value.class)
        .build();

    FileSystemDatasetRepository repo = new FileSystemDatasetRepository(fs.getConf(),
        testDirectory);
    Dataset<Value> writerDataset = repo.create("ns", "values", desc);
    DatasetWriter<Value> writer = writerDataset.newWriter();

    for (int i = 0; i < totalRecords; i++) {
      Value value = new Value();
      value.setId("r" + i);
      value.setValue(i);
      writer.write(value);
    }
    writer.close();

    readerDataset = repo.load("ns", "values", GenericRecord.class);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    fs.delete(testDirectory, true);
  }

  @Override
  public DatasetReader<GenericRecord> newReader() throws IOException {
    return readerDataset.newReader();
  }

  @Override
  public int getTotalRecords() {
    return totalRecords;
  }

  @Override
  public RecordValidator<GenericRecord> getValidator() {
    return new RecordValidator<GenericRecord>() {

      @Override
      public void validate(GenericRecord record, int recordNum) {
        Assert.assertEquals(GenericData.Record.class, record.getClass());
        Assert.assertEquals("r" + recordNum, record.get("id"));
        Assert.assertEquals((long) recordNum, record.get("value"));
      }
    };
  }

  @Test
  public void testExpectedSchema() {
    Schema expected = SchemaBuilder.record("org.kitesdk.data.event.thrift.Value")
        .fields()
        .optionalString("id")
        .requiredLong("value")
        .endRecord();
    // use normalization to ignore additional properties
    Assert.assertEquals("Schema should match expected structure",
        SchemaNormalization.parsingFingerprint64(expected),
        SchemaNormalization.parsingFingerprint64(
            readerDataset.getDescriptor().getSchema()));
  }
}
