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

package org.kitesdk.data.spi.hive;

import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.filesystem.TestFileSystemDatasetRepository;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.spi.MetadataProvider;

public class TestHiveManagedDatasetRepository extends TestFileSystemDatasetRepository {

  public TestHiveManagedDatasetRepository(boolean distributed) {
    super(distributed);
  }

  @Override
  public DatasetRepository newRepo(MetadataProvider provider) {
    // use null URI because TestDatasetRepositories expects no URI
    return new HiveManagedDatasetRepository(conf, provider);
  }

  @Override
  public MetadataProvider newProvider(Configuration conf) {
    return new HiveManagedMetadataProvider(conf);
  }

  private HiveMetaStoreClient client;

  @Before
  public void connectToHive() throws MetaException {
    this.client = new HiveMetaStoreClient(new HiveConf(conf, HiveConf.class));
  }

  @Before
  @After
  public void cleanHive() {
    // ensures all tables are removed
    MetaStoreUtil metastore = new MetaStoreUtil(getConfiguration());
    for (String database : metastore.getAllDatabases()) {
      for (String table : metastore.getAllTables(database)) {
        metastore.dropTable(database, table);
      }
      if (!"default".equals(database)) {
        metastore.dropDatabase(database, true);
      }
    }
  }

  @Test
  public void testTableExists() throws MetaException, TException {
    ensureCreated();
    HiveTestUtils.assertTableExists(client, NAMESPACE, NAME);
    HiveTestUtils.assertTableIsManaged(client, NAMESPACE, NAME);
  }

  @Override
  public void testCreatePath() throws IOException {
    // does not create paths, relies on Hive
  }

  @Override
  public void testDeleteRemovesDatasetPath() throws IOException {
    // does not delete paths, relies on Hive
  }

}
