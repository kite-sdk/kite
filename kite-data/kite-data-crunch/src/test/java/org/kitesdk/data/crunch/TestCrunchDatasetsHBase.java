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

import java.io.IOException;
import junit.framework.Assert;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.View;
import org.kitesdk.data.hbase.HBaseDatasetRepository;
import org.kitesdk.data.hbase.HBaseDatasetRepositoryTest;
import org.kitesdk.data.hbase.avro.AvroUtils;
import org.kitesdk.data.hbase.testing.HBaseTestUtils;
import org.kitesdk.data.spi.DatasetRepository;

import static org.junit.Assert.assertEquals;
import static org.kitesdk.data.spi.filesystem.DatasetTestUtilities.datasetSize;

public class TestCrunchDatasetsHBase {
  private static final String testGenericEntity;

  static {
    try {
      testGenericEntity = AvroUtils.inputStreamToString(TestCrunchDatasetsHBase.class
          .getResourceAsStream("/TestGenericEntity.avsc"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

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
    if (HBaseTestUtils.getMiniCluster() != null) {
      HBaseTestUtils.util.shutdownMiniHBaseCluster();
      HBaseTestUtils.util.shutdownMiniDFSCluster();
    }
  }

  @Before
  public void setUp() throws Exception {
    this.repo = new HBaseDatasetRepository.Builder()
        .configuration(HBaseTestUtils.getConf()).build();
  }

  @After
  public void after() throws Exception {
    HBaseTestUtils.util.truncateTable(Bytes.toBytes(tableName));
    HBaseTestUtils.util.truncateTable(Bytes.toBytes(managedTableName));
  }

  @Test
  @SuppressWarnings("deprecation") // still testing asTarget/asSource(Dataset)
  public void testGeneric() throws IOException {
    String datasetName = tableName + ".TestGenericEntity";

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral(testGenericEntity)
        .build();

    Dataset<GenericRecord> inputDataset = repo.create("in", descriptor);
    Dataset<GenericRecord> outputDataset = repo.create(datasetName, descriptor);

    writeRecords(inputDataset, 10);

    Pipeline pipeline = new MRPipeline(TestCrunchDatasetsHBase.class, HBaseTestUtils.getConf());
    PCollection<GenericRecord> data = pipeline.read(
        CrunchDatasets.asSource(inputDataset));
    pipeline.write(data, CrunchDatasets.asTarget(outputDataset), Target.WriteMode.APPEND);
    pipeline.run();

    checkRecords(outputDataset, 10, 0);
  }

  @Test
  @SuppressWarnings("deprecation") // still testing asTarget/asSource(Dataset)
  public void testSourceView() throws IOException {
    String datasetName = tableName + ".TestGenericEntity";

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral(testGenericEntity)
        .build();

    Dataset<GenericRecord> inputDataset = repo.create("in", descriptor);
    Dataset<GenericRecord> outputDataset = repo.create(datasetName, descriptor);

    writeRecords(inputDataset, 10);

    View<GenericRecord> inputView = inputDataset
        .from("part1", new Utf8("part1_2")).to("part1", new Utf8("part1_7"))
        .from("part2", new Utf8("part2_2")).to("part2", new Utf8("part2_7"));
    Assert.assertEquals(6, datasetSize(inputView));

    Pipeline pipeline = new MRPipeline(TestCrunchDatasetsHBase.class, HBaseTestUtils.getConf());
    PCollection<GenericRecord> data = pipeline.read(
        CrunchDatasets.asSource(inputView, GenericRecord.class));
    pipeline.write(data, CrunchDatasets.asTarget(outputDataset), Target.WriteMode.APPEND);
    pipeline.run();

    checkRecords(outputDataset, 6, 2);
  }

  private void writeRecords(Dataset<GenericRecord> dataset, int count) {
    DatasetWriter<GenericRecord> writer = dataset.newWriter();
    try {
      for (int i = 0; i < count; ++i) {
        GenericRecord entity = HBaseDatasetRepositoryTest.createGenericEntity(i);
        writer.write(entity);
      }
    } finally {
      writer.close();
    }
  }

  private void checkRecords(Dataset<GenericRecord> dataset, int count, int start) {
    int cnt = start;
    DatasetReader<GenericRecord> reader = dataset.newReader();
    try {
      for (GenericRecord entity : reader) {
        HBaseDatasetRepositoryTest.compareEntitiesWithUtf8(cnt, entity);
        cnt++;
      }
      assertEquals(count, cnt - start);
    } finally {
      reader.close();
    }
  }

}
