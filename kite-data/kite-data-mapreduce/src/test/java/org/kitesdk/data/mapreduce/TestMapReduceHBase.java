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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.filesystem.FileSystemDatasetRepository;
import org.kitesdk.data.hbase.HBaseDatasetRepository;
import org.kitesdk.data.hbase.HBaseDatasetRepositoryTest;
import org.kitesdk.data.hbase.avro.AvroUtils;
import org.kitesdk.data.hbase.testing.HBaseTestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestMapReduceHBase {

  private static final String testGenericEntity;

  static {
    try {
      testGenericEntity = AvroUtils.inputStreamToString(TestMapReduceHBase.class
          .getResourceAsStream("/TestGenericEntity.avsc"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

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

  private DatasetRepository fsRepo;
  private DatasetRepository repo;

  private static final String tableName = "testtable";
  private static final String managedTableName = "managed_schemas";

  @BeforeClass
  public static void beforeClass() throws Exception {
    HBaseTestUtils.getMiniCluster();
    // managed table should be created by HBaseDatasetRepository
    HBaseTestUtils.util.deleteTable(Bytes.toBytes(managedTableName));
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HBaseTestUtils.util.deleteTable(Bytes.toBytes(tableName));
  }

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fileSystem = FileSystem.get(conf);
    Path testDirectory = fileSystem.makeQualified(
        new Path(Files.createTempDir().getAbsolutePath()));
    this.fsRepo = new FileSystemDatasetRepository.Builder().configuration(conf)
        .rootDirectory(testDirectory).build();

    this.repo = new HBaseDatasetRepository.Builder()
        .configuration(HBaseTestUtils.getConf()).build();
  }

  @After
  public void after() throws Exception {
    HBaseTestUtils.util.truncateTable(Bytes.toBytes(tableName));
    HBaseTestUtils.util.truncateTable(Bytes.toBytes(managedTableName));
  }

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
    writer.open();
    try {
      for (int i = 0; i < 10; ++i) {
        GenericRecord entity = HBaseDatasetRepositoryTest.createGenericEntity(i);
        writer.write(entity);
      }
    } finally {
      writer.close();
    }

    job.setInputFormatClass(DatasetKeyInputFormat.class);
    DatasetKeyInputFormat.setRepositoryUri(job, repo.getUri());
    DatasetKeyInputFormat.setDatasetName(job, inputDataset.getName());

    job.setMapperClass(AvroKeyWrapperMapper.class);
    job.setMapOutputKeyClass(AvroKey.class);
    job.setMapOutputValueClass(NullWritable.class);
    AvroJob.setMapOutputKeySchema(job, new Schema.Parser().parse(testGenericEntity));

    job.setReducerClass(AvroKeyWrapperReducer.class);
    job.setOutputKeyClass(GenericData.Record.class);
    job.setOutputValueClass(Void.class);
    AvroJob.setOutputKeySchema(job, new Schema.Parser().parse(testGenericEntity));

    job.setOutputFormatClass(DatasetKeyOutputFormat.class);
    DatasetKeyOutputFormat.setRepositoryUri(job, repo.getUri());
    DatasetKeyOutputFormat.setDatasetName(job, outputDataset.getName());

    Assert.assertTrue(job.waitForCompletion(true));

    int cnt = 0;
    DatasetReader<GenericRecord> reader = outputDataset.newReader();
    reader.open();
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

}
