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
import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;

@Parameters(commandDescription = "Show the schema for a Dataset")
public class SchemaCommand extends BaseDatasetCommand {

  @Parameter(description = "<dataset name>")
  List<String> datasets;

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="UWF_NULL_FIELD",
      justification = "Field set by JCommander")
  @Parameter(names={"-o", "--output"}, description="Save schema avsc to path")
  String outputPath = null;

  @Parameter(names="--minimize",
      description="Minimize schema file size by eliminating white space")
  boolean minimize=false;

  public SchemaCommand(Logger console) {
    super(console);
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NP_NULL_ON_SOME_PATH",
      justification="Null case checked by precondition")
  public int run() throws IOException {
    Preconditions.checkArgument(
        datasets != null && !datasets.isEmpty(),
        "Missing dataset name");
    if (datasets.size() == 1) {
      String schema = load(datasets.get(0), Object.class)
          .getDataset()
          .getDescriptor()
          .getSchema()
          .toString(!minimize);
      output(schema, console, outputPath);

    } else {
      Preconditions.checkArgument(outputPath == null,
          "Cannot output multiple schemas to one file");
      for (String name : datasets) {
        console.info("Dataset \"{}\" schema: {}", name, load(name, Object.class)
            .getDataset()
            .getDescriptor()
            .getSchema()
            .toString(!minimize));
      }
    }
    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Print the schema for dataset \"users\" to standard out:",
        "users",
        "# Print the schema for a dataset URI to standard out:",
        "dataset:hbase:zk1,zk2/users",
        "# Save the schema for dataset \"users\" to user.avsc:",
        "users -o user.avsc"
    );
  }

}
