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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.Format;
import org.kitesdk.data.DatasetWriter;

@RunWith(Parameterized.class)
public class TestMapReduce extends FileSystemTestBase {

  private Dataset<GenericData.Record> inputDataset;
  private Dataset<GenericData.Record> outputDataset;

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

  @Before
  public void setUp() throws Exception {
    super.setUp();
    inputDataset = repo.create("in",
        new DatasetDescriptor.Builder()
            .property("kite.allow.csv", "true")
            .schema(STRING_SCHEMA)
            .format(format)
            .build(), GenericData.Record.class);
    outputDataset = repo.create("out",
        new DatasetDescriptor.Builder()
            .property("kite.allow.csv", "true")
            .schema(STATS_SCHEMA)
            .format(format)
            .build(), GenericData.Record.class);
  }

  @Test
  public void testJob() throws Exception {
    populateInputDataset();

    Job job = createJob();
    Assert.assertTrue(job.waitForCompletion(true));
    checkOutput(false);
  }

  @Test
  public void testJobEmptyView() throws Exception {
    Job job = createJob();
    Assert.assertTrue(job.waitForCompletion(true));
  }


  @Test(expected = DatasetException.class)
  public void testJobFailsWithExisting() throws Exception {
    populateInputDataset();
    populateOutputDataset(); // existing output will cause job to fail

    Job job = createJob();
    job.waitForCompletion(true);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testJobOverwrite() throws Exception {
    populateInputDataset();
    populateOutputDataset(); // existing output will be overwritten

    Job job = new Job();
    DatasetKeyInputFormat.configure(job).readFrom(inputDataset).withType(GenericData.Record.class);

    job.setMapperClass(LineCountMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setReducerClass(GenericStatsReducer.class);

    DatasetKeyOutputFormat.configure(job).overwrite(outputDataset).withType(GenericData.Record.class);

    Assert.assertTrue(job.waitForCompletion(true));
    checkOutput(false);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testJobAppend() throws Exception {
    populateInputDataset();
    populateOutputDataset(); // existing output will be overwritten

    Job job = new Job();
    DatasetKeyInputFormat.configure(job).readFrom(inputDataset).withType(GenericData.Record.class);

    job.setMapperClass(LineCountMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setReducerClass(GenericStatsReducer.class);

    DatasetKeyOutputFormat.configure(job).appendTo(outputDataset).withType(GenericData.Record.class);

    Assert.assertTrue(job.waitForCompletion(true));
    checkOutput(true);
  }

  private void populateInputDataset() {
    DatasetWriter<GenericData.Record> writer = inputDataset.newWriter();
    writer.write(newStringRecord("apple"));
    writer.write(newStringRecord("banana"));
    writer.write(newStringRecord("banana"));
    writer.write(newStringRecord("carrot"));
    writer.write(newStringRecord("apple"));
    writer.write(newStringRecord("apple"));
    writer.close();
  }

  private void populateOutputDataset() {
    DatasetWriter<GenericData.Record> writer = outputDataset.newWriter();
    writer.write(newStatsRecord(4, "date"));
    writer.close();
  }

  private void checkOutput(boolean existingPresent) {
    DatasetReader<GenericData.Record> reader = outputDataset.newReader();
    Map<String, Integer> counts = new HashMap<String, Integer>();
    for (GenericData.Record record : reader) {
      counts.put(record.get("name").toString(), (Integer) record.get("count"));
    }
    reader.close();

    Assert.assertEquals(3, counts.get("apple").intValue());
    Assert.assertEquals(2, counts.get("banana").intValue());
    Assert.assertEquals(1, counts.get("carrot").intValue());
    if (existingPresent) {
      Assert.assertEquals(4, counts.get("date").intValue());
    } else {
      Assert.assertNull(counts.get("date"));
    }
  }

  @SuppressWarnings("deprecation")
  private Job createJob() throws Exception {
    Job job = new Job();

    DatasetKeyInputFormat.configure(job).readFrom(inputDataset).withType(GenericData.Record.class);

    job.setMapperClass(LineCountMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setReducerClass(GenericStatsReducer.class);

    DatasetKeyOutputFormat.configure(job).writeTo(outputDataset).withType(GenericData.Record.class);

    return job;
  }

}
