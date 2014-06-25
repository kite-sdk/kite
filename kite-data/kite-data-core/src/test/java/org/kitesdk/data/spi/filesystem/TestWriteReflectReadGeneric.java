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
import java.io.IOException;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.kitesdk.data.*;
import org.kitesdk.data.spi.filesystem.DatasetTestUtilities.RecordValidator;

public class TestWriteReflectReadGeneric extends TestDatasetReaders<Record> {

  private static final int totalRecords = 100;
  protected static FileSystem fs = null;
  protected static Path testDirectory = null;
  protected static Dataset<Record> readerDataset;

  @BeforeClass
  public static void setup() throws IOException {
    Configuration conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testDirectory = new Path(Files.createTempDir().getAbsolutePath());
    FileSystemDatasetRepository repo = new FileSystemDatasetRepository(conf, testDirectory);
    Dataset<MyRecord> writerDataset = repo.create("test", new DatasetDescriptor.Builder()
                                   .schema(MyRecord.class)
                                   .build(), MyRecord.class);
    DatasetWriter<MyRecord> writer = writerDataset.newWriter();
    for (int i = 0; i < totalRecords; i++) {
      writer.write(new MyRecord(String.valueOf(i), i));
    }
    writer.close();

    readerDataset = repo.load("test", Record.class);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    fs.delete(testDirectory, true);
  }

  @Override
  public DatasetReader<Record> newReader() throws IOException {
    return readerDataset.newReader();
  }

  @Override
  public int getTotalRecords() {
    return totalRecords;
  }

  @Override
  public RecordValidator<Record> getValidator() {
    return new RecordValidator<Record>() {

      @Override
      public void validate(Record record, int recordNum) {
        Assert.assertEquals(String.valueOf(recordNum), record.get("text").toString());
        Assert.assertEquals(recordNum, record.get("value"));
      }
    };
  }

  public static class MyRecord {

    private String text;
    private int value;

    public MyRecord() {
    }

    public MyRecord(String text, int value) {
      this.text = text;
      this.value = value;
    }
  }
}
