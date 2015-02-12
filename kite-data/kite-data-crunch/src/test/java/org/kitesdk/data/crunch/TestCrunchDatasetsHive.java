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
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.After;
import org.kitesdk.data.spi.hive.HiveManagedDatasetRepository;
import org.kitesdk.data.spi.hive.MetaStoreUtil;
import org.kitesdk.data.spi.DatasetRepository;

public class TestCrunchDatasetsHive extends TestCrunchDatasets {
  public TestCrunchDatasetsHive(FileSystem fs) {
    super(fs);
  }

  @Override
  public DatasetRepository newRepo() {
    // Workaround for HIVE-7633 which is triggered by auto stats gather
    // since the external directory is created before the table is
    fileSystem.getConf().setBoolean(HiveConf.ConfVars.HIVESTATSAUTOGATHER.varname,
        false);
    return new HiveManagedDatasetRepository.Builder()
        .configuration(fileSystem.getConf())
        .rootDirectory(testDirectory).build();
  }

  @After
  public void cleanHCatalog() {
    // ensures all tables are removed
    MetaStoreUtil hcat = new MetaStoreUtil(fileSystem.getConf());

    for (String database: hcat.getAllDatabases()) {
      for (String tableName : hcat.getAllTables(database)) {
        hcat.dropTable(database, tableName);
      }
    }
  }
}
