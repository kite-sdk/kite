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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.LocalFileSystem;
import org.kitesdk.data.TestDatasetReaders;
import org.kitesdk.data.event.thrift.Value;
import org.kitesdk.data.spi.filesystem.DatasetTestUtilities.RecordValidator;

public class TestWriteGenericReadThrift extends TestDatasetReaders<Value> {

  private static final int totalRecords = 100;
  protected static FileSystem fs = null;
  protected static Path testDirectory = null;
  protected static Dataset<Value> readerDataset;

  @BeforeClass
  public static void setup() throws IOException {
    fs = LocalFileSystem.getInstance();
    testDirectory = new Path(Files.createTempDir().getAbsolutePath());

    DatasetDescriptor desc = new DatasetDescriptor.Builder()
        .schema(Value.class)
        .build();

    FileSystemDatasetRepository repo = new FileSystemDatasetRepository(fs.getConf(),
        testDirectory);
    Dataset<GenericRecord> writerDataset = repo.create("ns", "values", desc);
    DatasetWriter<GenericRecord> writer = writerDataset.newWriter();

    GenericData.Record record = new GenericData.Record(desc.getSchema());
    for (int i = 0; i < totalRecords; i++) {
      record.put("id", "r" + i);
      record.put("value", (long) i);
      writer.write(record);
    }
    writer.close();

    readerDataset = repo.load("ns", "values", Value.class);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    fs.delete(testDirectory, true);
  }

  @Override
  public DatasetReader<Value> newReader() throws IOException {
    return readerDataset.newReader();
  }

  @Override
  public int getTotalRecords() {
    return totalRecords;
  }

  @Override
  public RecordValidator<Value> getValidator() {
    return new RecordValidator<Value>() {

      @Override
      public void validate(Value record, int recordNum) {
        Assert.assertEquals("r" + recordNum, record.getId());
        Assert.assertEquals(recordNum, record.getValue());
      }
    };
  }
}
