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
import com.beust.jcommander.internal.Lists;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.kitesdk.data.spi.JsonUtil;
import org.kitesdk.data.spi.filesystem.CSVProperties;
import org.kitesdk.data.spi.filesystem.CSVUtil;
import org.slf4j.Logger;

@Parameters(commandDescription="Build a schema from a JSON data sample")
public class JSONSchemaCommand extends BaseCommand {

  @VisibleForTesting
  static final Charset SCHEMA_CHARSET = Charset.forName("utf8");

  private final Logger console;

  public JSONSchemaCommand(Logger console) {
    this.console = console;
  }

  @Parameter(description="<sample json path>")
  List<String> samplePaths;

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="UWF_NULL_FIELD",
      justification = "Field set by JCommander")
  @Parameter(names={"-o", "--output"}, description="Save schema avsc to path")
  String outputPath = null;

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="UWF_NULL_FIELD",
      justification = "Field set by JCommander")
  @Parameter(names={"--class", "--record-name"}, required = true,
      description="A name or class for the result schema")
  String recordName = null;

  @Parameter(names="--minimize",
      description="Minimize schema file size by eliminating white space")
  boolean minimize=false;

  @Override
  public int run() throws IOException {
    Preconditions.checkArgument(samplePaths != null && !samplePaths.isEmpty(),
        "Sample JSON path is required");
    Preconditions.checkArgument(samplePaths.size() == 1,
        "Only one JSON sample can be given");

    // assume fields are nullable by default, users can easily change this
    Schema sampleSchema = JsonUtil.inferSchema(
        open(samplePaths.get(0)), recordName, 10);

    if (sampleSchema != null) {
      output(sampleSchema.toString(!minimize), console, outputPath);
      return 0;
    } else {
      console.error("Sample file did not contain any records");
      return 1;
    }
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Print the schema for samples.json to standard out:",
        "samples.json --record-name Sample",
        "# Write schema to sample.avsc:",
        "samples.json -o sample.avsc --record-name Sample"
    );
  }
}
