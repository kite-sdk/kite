/*
 * Copyright 2015 Cloudera, Inc.
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

import com.google.common.collect.ImmutableList;
import junit.framework.Assert;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Format;
import org.kitesdk.data.spi.Compatibility;
import org.kitesdk.data.spi.SchemaValidationUtil;

@RunWith(Parameterized.class)
public class TestDatasetRecordWriter extends FileSystemTestBase {

  private Dataset<GenericData.Record> dataset;
  public static final Schema EVOLVED_STATS_SCHEMA = new Schema.Parser().parse(
      "{\"name\":\"stats\",\"type\":\"record\"," +
      "\"fields\":[" + 
        "{\"name\":\"id\",\"type\":[\"null\", \"string\"], \"default\": null}," +
        "{\"name\":\"count\",\"type\":\"int\"}," +
        "{\"name\":\"name\",\"type\":\"string\"}" +
      "]}");

  public TestDatasetRecordWriter(Format format) {
    super(format);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    dataset = repo.create("ns", "out",
        new DatasetDescriptor.Builder()
            .property("kite.allow.csv", "true")
            .schema(STATS_SCHEMA)
            .format(format)
            .build(), GenericData.Record.class);
  }

  @Test
  public void testBasicRecordWriter() {
    DatasetKeyOutputFormat.DatasetRecordWriter<GenericData.Record> recordWriter;
    recordWriter =
      new DatasetKeyOutputFormat.DatasetRecordWriter<GenericData.Record>(dataset);

    ImmutableList<Integer> counts = ImmutableList.of(1, 2, 3, 4, 5, 6, 7, 8, 9,
      10);

    for (Integer count : counts) {
      GenericData.Record record = new GenericData.Record(STATS_SCHEMA);
      record.put("count", count);
      record.put("name", "name"+count);

      recordWriter.write(record, null);
    }

    recordWriter.close(null);
  }
  
  @Test
  public void testRecordWriterWithEvolution() {
    DatasetKeyOutputFormat.DatasetRecordWriter<GenericData.Record> recordWriter;
    recordWriter =
      new DatasetKeyOutputFormat.DatasetRecordWriter<GenericData.Record>(dataset);

    ImmutableList<Integer> counts = ImmutableList.of(1, 2, 3, 4, 5, 6, 7, 8, 9,
      10);
    Assert.assertTrue(
      SchemaValidationUtil.canRead(EVOLVED_STATS_SCHEMA, STATS_SCHEMA));
    Assert.assertTrue(
      SchemaValidationUtil.canRead(STATS_SCHEMA, EVOLVED_STATS_SCHEMA));

    for (Integer count : counts) {
      GenericData.Record record = new GenericData.Record(EVOLVED_STATS_SCHEMA);
      record.put("count", count);
      record.put("name", "name"+count);
      record.put("id", "id"+count);

      recordWriter.write(record, null);
    }

    recordWriter.close(null);
  }
  
}
