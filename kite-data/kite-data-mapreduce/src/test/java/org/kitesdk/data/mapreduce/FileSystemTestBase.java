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

package org.kitesdk.data.mapreduce;

import com.google.common.io.Files;
import java.util.Arrays;
import java.util.Collection;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runners.Parameterized;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.DefaultConfiguration;
import org.kitesdk.data.spi.filesystem.FileSystemDatasetRepository;

public class FileSystemTestBase {

  private static Configuration original = null;

  @BeforeClass
  public static void saveDefaultConfiguration() {
    original = DefaultConfiguration.get();
  }

  @AfterClass
  public static void restoreDefaultConfiguration() {
    DefaultConfiguration.set(original);
  }

  public FileSystemTestBase(Format format) {
    this.format = format;
  }

  protected Format format;

  protected DatasetRepository repo;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][]{
      {Formats.AVRO},
      {Formats.PARQUET},
      {Formats.CSV}};
    return Arrays.asList(data);
  }

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fileSystem = FileSystem.get(conf);
    Path testDirectory = fileSystem.makeQualified(
        new Path(Files.createTempDir().getAbsolutePath()));
    this.repo = new FileSystemDatasetRepository.Builder()
        .configuration(conf)
        .rootDirectory(testDirectory)
        .build();
  }

  public static final Schema STATS_SCHEMA = new Schema.Parser().parse(
      "{\"name\":\"stats\",\"type\":\"record\"," +
      "\"fields\":[{\"name\":\"count\",\"type\":\"int\"}," +
      "{\"name\":\"name\",\"type\":\"string\"}]}");

  public static final Schema STRING_SCHEMA = new Schema.Parser().parse(
      "{\n" + "  \"name\": \"mystring\",\n" + "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "    { \"name\": \"text\", \"type\": \"string\" }\n" +
      "  ]\n" +
      "}\n");


  protected GenericData.Record newStringRecord(String text) {
    return new GenericRecordBuilder(STRING_SCHEMA).set("text", text).build();
  }

  protected GenericData.Record newStatsRecord(int count, String name) {
    return new GenericRecordBuilder(STATS_SCHEMA).set("count", count)
        .set("name", name).build();
  }
}
