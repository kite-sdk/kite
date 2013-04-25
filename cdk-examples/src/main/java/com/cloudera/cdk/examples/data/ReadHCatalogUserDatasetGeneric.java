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
package com.cloudera.cdk.examples.data;

import com.cloudera.data.Dataset;
import com.cloudera.data.DatasetReader;
import com.cloudera.data.DatasetRepository;
import com.cloudera.data.hcatalog.HCatalogDatasetRepository;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Read all the user objects from the users dataset using Avro generic records.
 */
public class ReadHCatalogUserDatasetGeneric extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    // Construct an HCatalog dataset repository using managed Hive tables
    DatasetRepository repo = new HCatalogDatasetRepository();

    // Get the users dataset
    Dataset users = repo.get("users");

    // Get a reader for the dataset and read all the users
    DatasetReader<GenericRecord> reader = users.getReader();
    try {
      reader.open();
      while (reader.hasNext()) {
        GenericRecord user = reader.read();
        System.out.println(user);
      }
    } finally {
      reader.close();
    }

    return 0;
  }

  public static void main(String... args) throws Exception {
    int rc = ToolRunner.run(new ReadHCatalogUserDatasetGeneric(), args);
    System.exit(rc);
  }
}
