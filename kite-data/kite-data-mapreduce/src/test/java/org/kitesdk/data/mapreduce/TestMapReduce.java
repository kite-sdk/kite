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
package org.kitesdk.data.mapreduce;

import com.google.common.io.Files;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.filesystem.FileSystemDatasetRepository;

@RunWith(Parameterized.class)
public class TestMapReduce {

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] {
      { Formats.AVRO },
      { Formats.PARQUET }
    };
    return Arrays.asList(data);
  }

  private Format format;

  public static final Schema STRING_SCHEMA =
      new Schema.Parser().parse("{\n" +
          "  \"name\": \"mystring\",\n" +
          "  \"type\": \"record\",\n" +
          "  \"fields\": [\n" +
          "    { \"name\": \"text\", \"type\": \"string\" }\n" +
          "  ]\n" +
          "}\n");
  public static final Schema STATS_SCHEMA =
      new Schema.Parser().parse("{\"name\":\"stats\",\"type\":\"record\","
          + "\"fields\":[{\"name\":\"count\",\"type\":\"int\"},"
          + "{\"name\":\"name\",\"type\":\"string\"}]}");

  private DatasetRepository repo;

  public TestMapReduce(Format format) {
    this.format = format;
  }

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fileSystem = FileSystem.get(conf);
    Path testDirectory = fileSystem.makeQualified(
        new Path(Files.createTempDir().getAbsolutePath()));
    this.repo = new FileSystemDatasetRepository.Builder().configuration(conf)
        .rootDirectory(testDirectory).build();
  }


  private static class LineCountMapper extends Mapper<GenericData.Record, NullWritable, Text, IntWritable> {
    @Override
    protected void map(GenericData.Record record, NullWritable value,
        Context context)
        throws IOException, InterruptedException {
      context.write(new Text(record.get("text").toString()), new IntWritable(1));
    }
  }

  private static class GenericStatsReducer
      extends Reducer<Text, IntWritable, GenericData.Record, Void> {

    @Override
    protected void reduce(Text line, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      GenericData.Record record = new GenericData.Record(STATS_SCHEMA);
      int sum = 0;
      for (IntWritable count : counts) {
        sum += count.get();
      }
      record.put("name", new Utf8(line.toString()));
      record.put("count", new Integer(sum));
      context.write(record, null);
    }
  }

  @Test
  public void testJob() throws Exception {
    Job job = new Job();

    Dataset<GenericData.Record> inputDataset = repo.create("in",
        new DatasetDescriptor.Builder().schema(STRING_SCHEMA).format(format).build());
    DatasetWriter<GenericData.Record> writer = inputDataset.newWriter();
    writer.open();
    writer.write(newStringRecord("apple"));
    writer.write(newStringRecord("banana"));
    writer.write(newStringRecord("banana"));
    writer.write(newStringRecord("carrot"));
    writer.write(newStringRecord("apple"));
    writer.write(newStringRecord("apple"));
    writer.close();

    job.setInputFormatClass(DatasetKeyInputFormat.class);
    DatasetKeyInputFormat.setRepositoryUri(job, repo.getUri());
    DatasetKeyInputFormat.setDatasetName(job, inputDataset.getName());

    job.setMapperClass(LineCountMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setReducerClass(GenericStatsReducer.class);

    Dataset<GenericData.Record> outputDataset = repo.create("out",
        new DatasetDescriptor.Builder().schema(STATS_SCHEMA).format(format).build());

    job.setOutputFormatClass(DatasetKeyOutputFormat.class);
    DatasetKeyOutputFormat.setRepositoryUri(job, repo.getUri());
    DatasetKeyOutputFormat.setDatasetName(job, outputDataset.getName());

    Assert.assertTrue(job.waitForCompletion(true));

    DatasetReader<GenericData.Record> reader = outputDataset.newReader();
    reader.open();
    Map<String, Integer> counts = new HashMap<String, Integer>();
    for (GenericData.Record record : reader) {
      counts.put(record.get("name").toString(), (Integer) record.get("count"));
    }
    reader.close();

    Assert.assertEquals(3, counts.get("apple").intValue());
    Assert.assertEquals(2, counts.get("banana").intValue());
    Assert.assertEquals(1, counts.get("carrot").intValue());

  }

  private GenericData.Record newStringRecord(String text) {
    return new GenericRecordBuilder(STRING_SCHEMA).set("text", text).build();
  }
}
