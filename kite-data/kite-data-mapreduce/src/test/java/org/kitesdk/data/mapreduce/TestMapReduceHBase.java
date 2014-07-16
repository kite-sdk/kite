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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.hbase.HBaseDatasetRepositoryTest;
import org.kitesdk.data.hbase.testing.HBaseTestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestMapReduceHBase extends HBaseTestBase {

  private static class AvroKeyWrapperMapper extends Mapper<GenericData.Record, Void,
      AvroKey<GenericData.Record>, NullWritable> {
    @Override
    protected void map(GenericData.Record record, Void value,
        Context context)
        throws IOException, InterruptedException {
      context.write(new AvroKey<GenericData.Record>(record), NullWritable.get());
    }
  }

  private static class AvroKeyWrapperReducer
      extends Reducer<AvroKey<GenericData.Record>, NullWritable, GenericData.Record, Void> {

    @Override
    protected void reduce(AvroKey<GenericData.Record> key,
        Iterable<NullWritable> values,
        Context context)
        throws IOException, InterruptedException {
      context.write(key.datum(), null);
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testJob() throws Exception {
    Job job = new Job(HBaseTestUtils.getConf());

    String datasetName = tableName + ".TestGenericEntity";

    Dataset<GenericRecord> inputDataset = repo.create("in",
        new DatasetDescriptor.Builder()
        .schemaLiteral(testGenericEntity).build());

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral(testGenericEntity)
        .build();
    Dataset<GenericRecord> outputDataset = repo.create(datasetName, descriptor);

    DatasetWriter<GenericRecord> writer = inputDataset.newWriter();
    try {
      for (int i = 0; i < 10; ++i) {
        GenericRecord entity = HBaseDatasetRepositoryTest.createGenericEntity(i);
        writer.write(entity);
      }
    } finally {
      writer.close();
    }

    DatasetKeyInputFormat.configure(job).readFrom(inputDataset);

    job.setMapperClass(AvroKeyWrapperMapper.class);
    job.setMapOutputKeyClass(AvroKey.class);
    job.setMapOutputValueClass(NullWritable.class);
    AvroJob.setMapOutputKeySchema(job, new Schema.Parser().parse(testGenericEntity));

    job.setReducerClass(AvroKeyWrapperReducer.class);
    job.setOutputKeyClass(GenericData.Record.class);
    job.setOutputValueClass(Void.class);
    AvroJob.setOutputKeySchema(job, new Schema.Parser().parse(testGenericEntity));

    DatasetKeyOutputFormat.configure(job).writeTo(outputDataset);

    Assert.assertTrue(job.waitForCompletion(true));

    int cnt = 0;
    DatasetReader<GenericRecord> reader = outputDataset.newReader();
    try {
      for (GenericRecord entity : reader) {
        HBaseDatasetRepositoryTest.compareEntitiesWithUtf8(cnt, entity);
        cnt++;
      }
      assertEquals(10, cnt);
    } finally {
      reader.close();
      assertFalse("Reader should be closed after calling close", reader.isOpen());
    }

  }

  @Test
  @SuppressWarnings("deprecation")
  public void testJobEmptyView() throws Exception {
    Job job = new Job(HBaseTestUtils.getConf());

    String datasetName = tableName + ".TestGenericEntity";

    Dataset<GenericRecord> inputDataset = repo.create("in",
        new DatasetDescriptor.Builder()
            .schemaLiteral(testGenericEntity).build());

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral(testGenericEntity)
        .build();
    Dataset<GenericRecord> outputDataset = repo.create(datasetName, descriptor);

    DatasetKeyInputFormat.configure(job).readFrom(inputDataset);

    job.setMapperClass(AvroKeyWrapperMapper.class);
    job.setMapOutputKeyClass(AvroKey.class);
    job.setMapOutputValueClass(NullWritable.class);
    AvroJob.setMapOutputKeySchema(job, new Schema.Parser().parse(testGenericEntity));

    job.setReducerClass(AvroKeyWrapperReducer.class);
    job.setOutputKeyClass(GenericData.Record.class);
    job.setOutputValueClass(Void.class);
    AvroJob.setOutputKeySchema(job, new Schema.Parser().parse(testGenericEntity));

    DatasetKeyOutputFormat.configure(job).writeTo(outputDataset);

    Assert.assertTrue(job.waitForCompletion(true));
  }
}
