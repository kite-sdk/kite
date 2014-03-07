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
package org.kitesdk.data.crunch;

import com.google.common.io.Files;
import java.io.IOException;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
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

@Ignore
public class TestCrunchDatasetsHBase {
  private static final String testGenericEntity;

  static {
    try {
      testGenericEntity = AvroUtils.inputStreamToString(HBaseDatasetRepositoryTest.class
          .getResourceAsStream("/TestGenericEntity.avsc"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  private DatasetRepository fsRepo;
  private DatasetRepository hbaseRepo;

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

    this.hbaseRepo = new HBaseDatasetRepository.Builder()
        .configuration(HBaseTestUtils.getConf()).build();
  }

  @After
  public void after() throws Exception {
    HBaseTestUtils.util.truncateTable(Bytes.toBytes(tableName));
    HBaseTestUtils.util.truncateTable(Bytes.toBytes(managedTableName));
  }

  @Test
  public void testGeneric() throws IOException {
    String datasetName = tableName + ".TestGenericEntity";

    Dataset<GenericRecord> inputDataset = fsRepo.create("in", new DatasetDescriptor.Builder()
        .schemaLiteral(testGenericEntity).build());

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral(testGenericEntity)
        .build();
    Dataset<GenericRecord> outputDataset = hbaseRepo.create(datasetName, descriptor);

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

    Pipeline pipeline = new MRPipeline(TestCrunchDatasetsHBase.class);
    PCollection<GenericRecord> data = pipeline.read(
        CrunchDatasets.asSource(inputDataset, GenericRecord.class));
    pipeline.write(data, CrunchDatasets.asTarget(outputDataset), Target.WriteMode.APPEND);
    pipeline.run();

    // ensure the new entities are what we expect with scan operations
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
    }

  }

  @AfterClass
  public static void delayForHadoop1() throws InterruptedException {
    Thread.sleep(5);
  }

}
