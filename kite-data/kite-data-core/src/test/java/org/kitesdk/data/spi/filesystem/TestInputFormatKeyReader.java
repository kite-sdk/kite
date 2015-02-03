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

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.LocalFileSystem;
import org.kitesdk.data.TestDatasetReaders;

public class TestInputFormatKeyReader extends TestDatasetReaders<LongWritable> {

  private static FileSystem localfs = null;
  private static Path userFile = new Path("target/test.text");
  private static List<String> lines = Lists.newArrayList(
      "line1", "line2", "line3");
  private static List<Long> positions = Lists.newArrayList(0L, 6L, 12L);

  @Override
  public DatasetReader<LongWritable> newReader() throws IOException {
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .property(InputFormatUtil.INPUT_FORMAT_CLASS_PROP,
            "org.apache.hadoop.mapreduce.lib.input.TextInputFormat")
        .property(InputFormatUtil.INPUT_FORMAT_RECORD_PROP, "key")
        .schema(Schema.create(Schema.Type.LONG))
        .build();
    return new InputFormatReader<LongWritable>(localfs, userFile, descriptor);
  }

  @Override
  public int getTotalRecords() {
    return lines.size();
  }

  @Override
  public DatasetTestUtilities.RecordValidator<LongWritable> getValidator() {
    return new DatasetTestUtilities.RecordValidator<LongWritable>() {
      @Override
      public void validate(LongWritable record, int recordNum) {
        System.err.println(record.toString());
        Assert.assertEquals((long) positions.get(recordNum), record.get());
      }
    };
  }

  @BeforeClass
  public static void setup() throws IOException {
    localfs = LocalFileSystem.getInstance();
    BufferedWriter writer = Files.newWriter(
        new File(userFile.toString()), Charset.forName("UTF-8"));
    for (String line : lines) {
      writer.write(line);
      writer.newLine();
    }
    writer.flush();
    writer.close();
  }
}
