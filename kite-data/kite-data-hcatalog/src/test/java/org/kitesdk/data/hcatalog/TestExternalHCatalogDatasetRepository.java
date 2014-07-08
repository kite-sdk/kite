/*
 * Copyright 2013 Cloudera.
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

package org.kitesdk.data.hcatalog;

import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.spi.PartitionKey;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.spi.PartitionedDataset;
import org.kitesdk.data.spi.filesystem.TestFileSystemDatasetRepository;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.hcatalog.impl.HCatalog;
import org.kitesdk.data.spi.Mergeable;
import org.kitesdk.data.spi.MetadataProvider;

public class TestExternalHCatalogDatasetRepository extends TestFileSystemDatasetRepository {

  public TestExternalHCatalogDatasetRepository(boolean distributed) {
    super(distributed);
  }

  @Override
  public DatasetRepository newRepo(MetadataProvider provider) {
    // use null URI because TestDatasetRepositories expects no URI
    return new HCatalogExternalDatasetRepository(conf, testDirectory, provider);
  }

  @Override
  public MetadataProvider newProvider(Configuration conf) {
    return new HCatalogExternalMetadataProvider(conf, testDirectory);
  }

  private HiveMetaStoreClient client;

  @Before
  public void connectToHive() throws MetaException {
    this.client = new HiveMetaStoreClient(new HiveConf(conf, HiveConf.class));
  }

  @After
  public void cleanHCatalog() {
    // ensures all tables are removed
    HCatalog hcat = new HCatalog(conf);
    for (String tableName : hcat.getAllTables("default")) {
      hcat.dropTable("default", tableName);
    }
  }

  @Test
  public void testTableExists() throws MetaException, TException {
    ensureCreated();
    HiveTestUtils.assertTableExists(client, "default", NAME);
    HiveTestUtils.assertTableIsExternal(client, "default", NAME);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testNewPartitionIsVisibleToHive() throws Exception {
    final String NAME2 = "test2";

    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder()
        .hash("username", 2).build();

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(testSchema)
        .partitionStrategy(partitionStrategy)
        .build();

    Dataset<GenericRecord> dataset = repo.create(NAME2, descriptor);

    HiveTestUtils.assertTableExists(client, "default", NAME2);
    HiveTestUtils.assertTableIsExternal(client, "default", NAME2);
    Assert.assertTrue("No partitions yet",
        client.listPartitionNames("default", NAME2, (short) 10).isEmpty());

    writeRecord(dataset, 0);

    Assert.assertEquals("Should be one partition", 1,
        client.listPartitionNames("default", NAME2, (short) 10).size());

  }

  @SuppressWarnings({"deprecation", "unchecked"})
  @Test
  public void testMerge() throws Exception {
    final String NAME2 = "test2";
    final String NAME3 = "test3";

    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder()
        .hash("username", 2).build();

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(testSchema)
        .partitionStrategy(partitionStrategy)
        .build();

    Dataset<GenericRecord> dataset = repo.create(NAME2, descriptor);

    HiveTestUtils.assertTableExists(client, "default", NAME2);
    HiveTestUtils.assertTableIsExternal(client, "default", NAME2);
    Assert.assertTrue("No partitions yet",
        client.listPartitionNames("default", NAME2, (short) 10).isEmpty());

    writeRecord(dataset, 0);

    Assert.assertEquals("Should be one partition", 1,
        client.listPartitionNames("default", NAME2, (short) 10).size());

    Dataset<GenericRecord> dsUpdate = repo.create(NAME3, descriptor);

    writeRecord(dsUpdate, 1);

    Assert.assertTrue(dataset instanceof Mergeable);
    ((Mergeable<Dataset<GenericRecord>>) dataset).merge(dsUpdate);

    Assert.assertEquals("Should be two partitions", 2,
        client.listPartitionNames("default", NAME2, (short) 10).size());

  }

  private void writeRecord(Dataset<GenericRecord> dataset, int partition) {
    PartitionKey key = new PartitionKey(partition);
    DatasetWriter<GenericRecord> writer =
        ((PartitionedDataset<GenericRecord>) dataset).getPartition(key, true).newWriter();
    try {
      GenericRecordBuilder recordBuilder = new GenericRecordBuilder(
          dataset.getDescriptor().getSchema())
          .set("username", partition + "").set("email", partition + "@example.com");
      writer.write(recordBuilder.build());
    } finally {
      writer.close();
    }

  }
}
