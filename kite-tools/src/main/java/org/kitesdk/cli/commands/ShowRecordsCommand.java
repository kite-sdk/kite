/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetRepository;
import org.slf4j.Logger;

@Parameters(commandDescription = "Print the first n records in a Dataset")
public class ShowRecordsCommand extends BaseDatasetCommand implements Configurable {

  private Configuration conf;

  @Parameter(description = "<dataset name>")
  List<String> datasetNames;

  @Parameter(names={"-n", "--num-records"},
      description="The number of records to print")
  int numRecords = 10;

  public ShowRecordsCommand(Logger console) {
    super(console);
  }

  @Override
  public int run() throws IOException {
    Preconditions.checkArgument(
        datasetNames != null && !datasetNames.isEmpty(),
        "Missing dataset name");
    Preconditions.checkArgument(datasetNames.size() == 1,
        "Only one dataset name can be given");

    DatasetRepository repo = getDatasetRepository();
    // TODO: CDK-92: always use GenericRecord to have consistent record strings

    Dataset<Object> dataset = repo.load(datasetNames.get(0));
    DatasetReader<Object> reader = dataset.newReader();
    boolean threw = true;
    try {
      reader.open();
      int i = 0;
      for (Object record : reader) {
        if (i >= numRecords) {
          break;
        }
        console.info(record.toString());
        i += 1;
      }
      threw = false;
    } finally {
      Closeables.close(reader, threw);
    }

    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Show the first 10 records in dataset \"users\":",
        "users",
        "# Show the first 50 records in dataset \"users\":",
        "users -n 50"
    );
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
