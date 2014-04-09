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
import org.kitesdk.data.DatasetRepository;
import org.slf4j.Logger;

@Parameters(commandDescription = "Delete a dataset and its metadata")
public class DeleteDatasetCommand extends BaseDatasetCommand {

  @Parameter(description = "<dataset names>")
  List<String> datasetNames;

  public DeleteDatasetCommand(Logger console) {
    super(console);
  }

  @Override
  public int run() throws IOException {
    DatasetRepository repo = getDatasetRepository();

    if (datasetNames == null || datasetNames.isEmpty()) {
      throw new IllegalArgumentException("No dataset names were specified.");
    }

    for (String datasetName : datasetNames) {
      repo.delete(datasetName);
      console.debug("Deleted dataset {}", datasetName);
    }

    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Delete all data and metadata for the dataset \"users\":",
        "users"
    );
  }

}
