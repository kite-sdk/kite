/**
 * Copyright 2014 Cloudera Inc.
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

import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.kitesdk.data.hcatalog.impl.HCatalog;
import org.kitesdk.data.hcatalog.HCatalogDatasetRepository;
import org.kitesdk.data.spi.DatasetRepository;

public class TestCrunchDatasetsHCatalog extends TestCrunchDatasets {
  public TestCrunchDatasetsHCatalog(FileSystem fs) {
    super(fs);
  }

  @Override
  public DatasetRepository newRepo() {
    return new HCatalogDatasetRepository.Builder()
        .configuration(fileSystem.getConf())
        .rootDirectory(testDirectory).build();
  }

  @After
  public void cleanHCatalog() {
    // ensures all tables are removed
    HCatalog hcat = new HCatalog(fileSystem.getConf());
    for (String tableName : hcat.getAllTables("default")) {
      hcat.dropTable("default", tableName);
    }
  }
}
