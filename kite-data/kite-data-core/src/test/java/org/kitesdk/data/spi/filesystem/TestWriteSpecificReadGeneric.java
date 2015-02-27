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
import org.kitesdk.data.event.StandardEvent;
import org.kitesdk.data.spi.filesystem.DatasetTestUtilities.RecordValidator;

import java.io.IOException;

public class TestWriteSpecificReadGeneric extends TestDatasetReaders<GenericData.Record> {

  private static final int totalRecords = 100;
  protected static FileSystem fs = null;
  protected static Path testDirectory = null;
  protected static Dataset<GenericData.Record> readerDataset;

  @BeforeClass
  public static void setup() throws IOException {
    fs = LocalFileSystem.getInstance();
    testDirectory = new Path(Files.createTempDir().getAbsolutePath());
    FileSystemDatasetRepository repo = new FileSystemDatasetRepository(fs.getConf(),
        testDirectory);
    Dataset<StandardEvent> writerDataset = repo.create("ns", "test", new DatasetDescriptor.Builder()
                                   .schema(StandardEvent.class)
                                   .build(), StandardEvent.class);
    DatasetWriter<StandardEvent> writer = writerDataset.newWriter();
    for (long i = 0; i < totalRecords; i++) {
      String text = String.valueOf(i);
      writer.write(new StandardEvent(text, text, i, text, text, i));
    }
    writer.close();

    readerDataset = repo.load("ns", "test", GenericData.Record.class);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    fs.delete(testDirectory, true);
  }

  @Override
  public DatasetReader<GenericData.Record> newReader() throws IOException {
    return readerDataset.newReader();
  }

  @Override
  public int getTotalRecords() {
    return totalRecords;
  }

  @Override
  public RecordValidator<GenericData.Record> getValidator() {
    return new RecordValidator<GenericData.Record>() {

      @Override
      public void validate(GenericData.Record record, int recordNum) {
        Assert.assertEquals(GenericData.Record.class, record.getClass());
        Assert.assertEquals(String.valueOf(recordNum), record.get("event_initiator").toString());
        Assert.assertEquals(String.valueOf(recordNum), record.get("event_name").toString());
        Assert.assertEquals(Long.valueOf(recordNum), record.get("user_id"));
        Assert.assertEquals(String.valueOf(recordNum), record.get("session_id").toString());
        Assert.assertEquals(String.valueOf(recordNum), record.get("ip").toString());
        Assert.assertEquals(Long.valueOf(recordNum), record.get("timestamp"));
      }
    };
  }
}
