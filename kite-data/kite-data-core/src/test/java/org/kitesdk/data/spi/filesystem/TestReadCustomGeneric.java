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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.TestDatasetReaders;
import org.kitesdk.data.spi.filesystem.DatasetTestUtilities.RecordValidator;

public class TestReadCustomGeneric extends TestDatasetReaders<TestGenericRecord> {

  private static final int totalRecords = 100;
  protected static FileSystem fs = null;
  protected static Path testDirectory = null;
  protected static Dataset<TestGenericRecord> readerDataset;

  @BeforeClass
  public static void setup() throws IOException {
    Configuration conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testDirectory = new Path(Files.createTempDir().getAbsolutePath());
    FileSystemDatasetRepository repo = new FileSystemDatasetRepository(conf, testDirectory);
    Dataset<MyRecord> writerDataset = repo.create("ns", "test", new DatasetDescriptor.Builder()
                                   .schema(MyRecord.class)
                                   .build(), MyRecord.class);
    DatasetWriter<MyRecord> writer = writerDataset.newWriter();
    for (int i = 0; i < totalRecords; i++) {
      writer.write(new MyRecord(String.valueOf(i), i));
    }
    writer.close();

    readerDataset = repo.load("ns", "test", TestGenericRecord.class);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    fs.delete(testDirectory, true);
  }

  @Override
  public DatasetReader<TestGenericRecord> newReader() throws IOException {
    return readerDataset.newReader();
  }

  @Override
  public int getTotalRecords() {
    return totalRecords;
  }

  @Override
  public RecordValidator<TestGenericRecord> getValidator() {
    return new RecordValidator<TestGenericRecord>() {

      @Override
      public void validate(TestGenericRecord record, int recordNum) {
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
