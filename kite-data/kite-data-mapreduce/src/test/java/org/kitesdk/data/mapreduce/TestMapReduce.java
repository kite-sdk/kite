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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.Format;
import org.kitesdk.data.DatasetWriter;

@RunWith(Parameterized.class)
public class TestMapReduce extends FileSystemTestBase {

  public TestMapReduce(Format format) {
    super(format);
  }

  private static class LineCountMapper
      extends Mapper<GenericData.Record, Void, Text, IntWritable> {
    @Override
    protected void map(GenericData.Record record, Void value,
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
  @SuppressWarnings("deprecation")
  public void testJob() throws Exception {
    Job job = new Job();

    Dataset<GenericData.Record> inputDataset = repo.create("in",
        new DatasetDescriptor.Builder()
            .property("kite.allow.csv", "true")
            .schema(STRING_SCHEMA)
            .format(format)
            .build(), GenericData.Record.class);
    DatasetWriter<GenericData.Record> writer = inputDataset.newWriter();
    writer.write(newStringRecord("apple"));
    writer.write(newStringRecord("banana"));
    writer.write(newStringRecord("banana"));
    writer.write(newStringRecord("carrot"));
    writer.write(newStringRecord("apple"));
    writer.write(newStringRecord("apple"));
    writer.close();

    DatasetKeyInputFormat.configure(job).readFrom(inputDataset).withType(GenericData.Record.class);

    job.setMapperClass(LineCountMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setReducerClass(GenericStatsReducer.class);

    Dataset<GenericData.Record> outputDataset = repo.create("out",
        new DatasetDescriptor.Builder()
            .property("kite.allow.csv", "true")
            .schema(STATS_SCHEMA)
            .format(format)
            .build(), GenericData.Record.class);

    DatasetKeyOutputFormat.configure(job).writeTo(outputDataset).withType(GenericData.Record.class);

    Assert.assertTrue(job.waitForCompletion(true));

    DatasetReader<GenericData.Record> reader = outputDataset.newReader();
    Map<String, Integer> counts = new HashMap<String, Integer>();
    for (GenericData.Record record : reader) {
      counts.put(record.get("name").toString(), (Integer) record.get("count"));
    }
    reader.close();

    Assert.assertEquals(3, counts.get("apple").intValue());
    Assert.assertEquals(2, counts.get("banana").intValue());
    Assert.assertEquals(1, counts.get("carrot").intValue());

  }

  @Test
  @SuppressWarnings("deprecation")
  public void testJobEmptyView() throws Exception {
    Job job = new Job();

    Dataset<GenericData.Record> inputDataset = repo.create("in",
        new DatasetDescriptor.Builder()
            .property("kite.allow.csv", "true")
            .schema(STRING_SCHEMA)
            .format(format)
            .build(), GenericData.Record.class);

    DatasetKeyInputFormat.configure(job).readFrom(inputDataset).withType(GenericData.Record.class);

    job.setMapperClass(LineCountMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setReducerClass(GenericStatsReducer.class);

    Dataset<GenericData.Record> outputDataset = repo.create("out",
        new DatasetDescriptor.Builder()
            .property("kite.allow.csv", "true")
            .schema(STATS_SCHEMA)
            .format(format)
            .build(), GenericData.Record.class);

    DatasetKeyOutputFormat.configure(job).writeTo(outputDataset).withType(GenericData.Record.class);

    Assert.assertTrue(job.waitForCompletion(true));
  }

}
