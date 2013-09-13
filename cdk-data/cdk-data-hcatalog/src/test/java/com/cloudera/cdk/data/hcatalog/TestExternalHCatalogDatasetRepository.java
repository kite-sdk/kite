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

package com.cloudera.cdk.data.hcatalog;

import com.cloudera.cdk.data.MetadataProvider;
import com.cloudera.cdk.data.filesystem.TestFileSystemDatasetRepository;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestExternalHCatalogDatasetRepository extends TestFileSystemDatasetRepository {

  public TestExternalHCatalogDatasetRepository(String mode) {
    super(mode);
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
    HCatalog hcat = new HCatalog();
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

  @Override
  public void testUpdateFailsWithFormatChange() {
    // updates are not supported
  }

  @Override
  public void testUpdateFailsWithPartitionStrategyChange() {
    // updates are not supported
  }

  @Override
  public void testUpdateFailsWithLocationChange() {
    // updates are not supported
  }

  @Override
  public void testUpdateFailsWithIncompatibleSchemaChange() {
    // updates are not supported
  }

  @Override
  public void testUpdateSuccessfulWithCompatibleSchemaChange() {
    // updates are not supported
  }

}
