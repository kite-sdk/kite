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
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.MetadataProvider;
import org.kitesdk.data.PartitionKey;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.filesystem.TestFileSystemDatasetRepository;
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

public class TestExternalHCatalogDatasetRepository extends TestFileSystemDatasetRepository {

  public TestExternalHCatalogDatasetRepository(boolean distributed) {
    super(distributed);
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

    PartitionKey key0 = partitionStrategy.partitionKey(0);
    DatasetWriter<GenericRecord> writer = dataset.getPartition(key0, true).newWriter();
    writer.open();
    try {
      GenericRecordBuilder recordBuilder = new GenericRecordBuilder(
          dataset.getDescriptor().getSchema())
          .set("username", "0").set("email", "0@example.com");
      writer.write(recordBuilder.build());
    } finally {
      writer.close();
    }

    Assert.assertEquals("Should be one partition", 1,
        client.listPartitionNames("default", NAME2, (short) 10).size());

  }
}
