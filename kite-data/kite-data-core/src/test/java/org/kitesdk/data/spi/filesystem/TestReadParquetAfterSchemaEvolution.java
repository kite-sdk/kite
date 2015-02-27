/*
 * Copyright 2014 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spi.filesystem;

import com.google.common.io.Files;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.kitesdk.data.*;
import org.kitesdk.data.event.Value;
import org.kitesdk.data.spi.filesystem.DatasetTestUtilities.RecordValidator;

import java.io.IOException;

public class TestReadParquetAfterSchemaEvolution extends TestDatasetReaders<GenericRecord> {

  private static final int totalRecords = 100;
  protected static FileSystem fs = null;
  protected static Path testDirectory = null;
  protected static Dataset<GenericRecord> readerDataset;

  @BeforeClass
  public static void setup() throws IOException {
    fs = LocalFileSystem.getInstance();
    testDirectory = new Path(Files.createTempDir().getAbsolutePath());
    FileSystemDatasetRepository repo = new FileSystemDatasetRepository(fs.getConf(),
        testDirectory);
    Dataset<GenericRecord> writerDataset = repo.create("ns", "test", new DatasetDescriptor.Builder()
                                   .schema(DatasetTestUtilities.OLD_VALUE_SCHEMA)
                                   .format(Formats.PARQUET)
                                   .build(), GenericRecord.class);
    
    DatasetWriter<GenericRecord> writer = writerDataset.newWriter();
    
    GenericRecord record = new GenericData.Record(DatasetTestUtilities.OLD_VALUE_SCHEMA);
    for (long i = 0; i < totalRecords; i++) {
      record.put("value", Long.valueOf(i));
      writer.write(record);
    }
    writer.close();
    
    repo.update("ns", "test", new DatasetDescriptor.Builder(writerDataset.getDescriptor())
      .schema(Value.class).build());

    readerDataset = repo.load("ns", "test", GenericRecord.class);
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
        Assert.assertEquals(null, record.get("id"));
        Assert.assertEquals(Long.valueOf(recordNum), record.get("value"));
      }
    };
  }
}
