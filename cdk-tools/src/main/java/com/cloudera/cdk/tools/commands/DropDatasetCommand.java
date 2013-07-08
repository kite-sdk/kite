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
package com.cloudera.cdk.tools.commands;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.cloudera.cdk.data.DatasetRepository;
import java.io.IOException;
import java.util.List;

@Parameters(commandDescription = "Delete a dataset and its metadata")
public class DropDatasetCommand extends BaseDatasetCommand {

  @Parameter(description = "<dataset names>")
  List<String> datasetNames;

  @Override
  public int run() throws IOException {
    DatasetRepository repo = getDatasetRepository();

    for (String datasetName : datasetNames) {
      repo.drop(datasetName);
    }

    return 0;
  }
}
