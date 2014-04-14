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
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.DatasetRepository;
import org.slf4j.Logger;

@Parameters(commandDescription = "Show the schema for a Dataset")
public class SchemaCommand extends BaseDatasetCommand implements Configurable {

  @VisibleForTesting
  static final Charset SCHEMA_CHARSET = Charset.forName("utf8");

  private Configuration conf;

  @Parameter(description = "<dataset name>")
  List<String> datasetNames;

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
  public int run() throws IOException {
    Preconditions.checkArgument(
        datasetNames != null && !datasetNames.isEmpty(),
        "Missing dataset name");
    DatasetRepository repo = getDatasetRepository();
    if (datasetNames.size() == 1) {
      String schema = repo.load(datasetNames.get(0))
          .getDescriptor()
          .getSchema()
          .toString(!minimize);
      if (outputPath == null || "-".equals(outputPath)) {
        console.info(schema);
      } else {
        Path out = new Path(outputPath);
        FileSystem outFS = out.getFileSystem(conf);
        FSDataOutputStream outgoing = outFS.create(out, true /* overwrite */ );
        try {
          outgoing.write(schema.getBytes(SCHEMA_CHARSET));
        } finally {
          outgoing.close();
        }
      }

    } else {
      Preconditions.checkArgument(outputPath == null,
          "Cannot output multiple schemas to one file");
      for (String name : datasetNames) {
        console.info("Dataset \"{}\" schema: {}", name, repo.load(name)
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
        "# Save the schema for dataset \"users\" to user.avsc:",
        "users -o user.avsc"
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
