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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import java.io.IOException;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.View;
import org.slf4j.Logger;

@Parameters(commandDescription = "Print the first n records in a Dataset")
public class ShowRecordsCommand extends BaseDatasetCommand {

  @Parameter(description = "<dataset name>")
  List<String> datasets;

  @Parameter(names={"-n", "--num-records"},
      description="The number of records to print")
  int numRecords = 10;

  public ShowRecordsCommand(Logger console) {
    super(console);
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NP_NULL_ON_SOME_PATH",
      justification="Null case checked by precondition")
  public int run() throws IOException {
    Preconditions.checkArgument(
        datasets != null && !datasets.isEmpty(),
        "Missing dataset name");
    Preconditions.checkArgument(datasets.size() == 1,
        "Only one dataset name can be given");

    View<Object> dataset = load(datasets.get(0), Object.class);
    DatasetReader<Object> reader = null;
    boolean threw = true;
    try {
      reader = dataset.newReader();
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
        "users -n 50",
        "# Show the first 10 records for dataset URI:",
        "dataset:hbase:zk1,zk2/users",
        "# Show the first 10 records for view URI:",
        "view:hbase:zk1,zk2/users?username=u1"
    );
  }

}
