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
package com.cloudera.cdk.data.hbase.avro;

import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.DatasetReader;
import com.cloudera.cdk.data.RandomAccessDataset;
import com.cloudera.cdk.data.TestDatasetReaders;
import com.cloudera.cdk.data.filesystem.DatasetTestUtilities;
import com.cloudera.cdk.data.hbase.HBaseDatasetRepository;
import com.cloudera.cdk.data.hbase.HBaseDatasetRepositoryTest;
import com.cloudera.cdk.data.hbase.testing.HBaseTestUtils;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class HBaseDatasetReaderTest extends TestDatasetReaders<GenericRecord> {

  private static final String tableName = "testtable";
  private static final String managedTableName = "managed_schemas";

  private static RandomAccessDataset<GenericRecord> dataset;

  @BeforeClass
  public static void beforeClass() throws Exception {
    HBaseTestUtils.getMiniCluster();
    // managed table should be created by HBaseDatasetRepository
    HBaseTestUtils.util.deleteTable(Bytes.toBytes(managedTableName));
    HBaseDatasetRepository repo = new HBaseDatasetRepository.Builder()
        .configuration(HBaseTestUtils.getConf()).build();
    String testGenericEntity = AvroUtils.inputStreamToString(
        HBaseDatasetRepositoryTest.class.getResourceAsStream("/TestGenericEntity.avsc"));
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral(testGenericEntity)
        .build();
    dataset = repo.create("testtable", descriptor);
    for (int i = 0; i < 10; i++) {
      dataset.put(HBaseDatasetRepositoryTest.createGenericEntity(i));
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HBaseTestUtils.util.deleteTable(Bytes.toBytes(tableName));
    HBaseTestUtils.util.truncateTable(Bytes.toBytes(managedTableName));
  }

  @Override
  public DatasetReader<GenericRecord> newReader() throws IOException {
    return dataset.newReader();
  }

  @Override
  public int getTotalRecords() {
    return 10;
  }

  @Override
  public DatasetTestUtilities.RecordValidator<GenericRecord> getValidator() {
    return new DatasetTestUtilities.RecordValidator<GenericRecord>() {
      @Override
      public void validate(GenericRecord record, int recordNum) {
        HBaseDatasetRepositoryTest.compareEntitiesWithUtf8(recordNum, record);
      }
    };
  }
}
