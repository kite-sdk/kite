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
package org.kitesdk.cli.commands;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.spi.DatasetRepository;
import org.slf4j.Logger;

@Parameters(commandDescription = "Delete a dataset and its metadata")
public class DeleteDatasetCommand extends BaseDatasetCommand {

  @Parameter(description = "<dataset names>")
  List<String> datasets;

  public DeleteDatasetCommand(Logger console) {
    super(console);
  }

  @Override
  public int run() throws IOException {
    DatasetRepository repo = getDatasetRepository();

    if (datasets == null || datasets.isEmpty()) {
      throw new IllegalArgumentException("No dataset names were specified.");
    }

    for (String datasetUriOrName : datasets) {
      if (isDataUri(datasets.get(0))) {
        Datasets.delete(datasetUriOrName);
      } else {
        repo.delete(namespace, datasetUriOrName);
      }
      console.debug("Deleted dataset {}", datasetUriOrName);
    }

    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Delete all data and metadata for the dataset \"users\":",
        "users",
        "# Delete all data and metadata for a dataset by URI:",
        "dataset:hbase:zk1,zk2/users"
    );
  }

}
