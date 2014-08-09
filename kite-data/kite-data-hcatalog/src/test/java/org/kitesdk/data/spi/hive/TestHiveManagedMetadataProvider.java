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

import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.kitesdk.data.spi.MetadataProvider;
import org.kitesdk.data.spi.TestMetadataProviders;

public class TestHiveManagedMetadataProvider extends TestMetadataProviders {

  public TestHiveManagedMetadataProvider(boolean distributed) {
    super(distributed);
  }

  @Override
  public MetadataProvider newProvider(Configuration conf) {
    return new HiveManagedMetadataProvider(conf);
  }

  @Before
  @After
  public void cleanHCatalog() {
    // ensures all tables are removed
    MetaStoreUtil metastore = new MetaStoreUtil(conf);
    for (String tableName : metastore.getAllTables(NAMESPACE)) {
      metastore.dropTable(NAMESPACE, tableName);
    }
  }

  @Override
  public void testCreateWithLocation() throws URISyntaxException {
    // not valid for managed tables
  }
}
