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
package org.kitesdk.data.spi.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.spi.MetadataProvider;

class HiveExternalDatasetRepository extends HiveAbstractDatasetRepository {

  /**
   * Create an HCatalog dataset repository with external tables.
   */
  HiveExternalDatasetRepository(Configuration conf, Path rootDirectory) {
    super(conf, rootDirectory,
        new HiveExternalMetadataProvider(conf, rootDirectory));
  }

  /**
   * Create an HCatalog dataset repository with external tables.
   */
  HiveExternalDatasetRepository(Configuration conf, Path rootDirectory,
                                MetadataProvider provider) {
    super(conf, rootDirectory, provider);
  }

}
