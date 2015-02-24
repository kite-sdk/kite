/*
 * Copyright 2014 Cloudera Inc.
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
import java.net.URL;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;

@Parameters(commandDescription = "Build a log4j config to log events to a dataset")
public class Log4jConfigCommand extends BaseDatasetCommand {

  @Parameter(description = "Dataset name or URI", required = true)
  List<String> datasetName;

  @Parameter(names={"--host"}, description = "Flume hostname", required = true)
  String hostname;

  @Parameter(names={"--port"}, description = "Flume port")
  int port = 41415;

  @Parameter(names={"--class", "--package"}, description = "Java class/package to log from")
  String packageName;

  @Parameter(names={"--log-all"}, description = "Configure the root logger to send to Flume")
  boolean logAll;

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="UWF_NULL_FIELD",
      justification = "Field set by JCommander")
  @Parameter(names={"-o", "--output"}, description="Save logging config to path")
  String outputPath = null;

  public Log4jConfigCommand(Logger console) {
    super(console);
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NP_NULL_ON_SOME_PATH",
      justification="Null case checked by precondition")
  public int run() throws IOException {
    Preconditions.checkArgument(
        datasetName != null && !datasetName.isEmpty(),
        "Missing dataset uri");

    Preconditions.checkArgument(
        hostname != null,
        "Missing Flume hostname");

    Preconditions.checkArgument(
        packageName != null || logAll,
        "Must provide a class/package name or specify --log-all");

    URL schemaUrl = load(datasetName.get(0), GenericRecord.class).getDataset()
        .getDescriptor().getSchemaUrl();
    if (schemaUrl == null) {
      console.warn("Warning: The dataset {} does not have a schema URL. The schema will be sent with each event.", datasetName.get(0));
    }

    StringBuilder sb = new StringBuilder();
    if (logAll) {
      sb.append("# Log events from all classes:\n");
      sb.append("log4j.rootLogger = INFO, flume\n");
      sb.append("\n");
    }
    sb.append("log4j.appender.flume = org.apache.flume.clients.log4jappender.Log4jAppender\n");
    sb.append("log4j.appender.flume.Hostname = ").append(hostname).append("\n");
    sb.append("log4j.appender.flume.Port = ").append(port).append("\n");
    sb.append("log4j.appender.flume.UnsafeMode = true").append("\n");
    if (schemaUrl != null) {
      sb.append("log4j.appender.flume.AvroSchemaUrl = ").append(schemaUrl).append("\n");
    }
    if (packageName != null) {
      sb.append("\n");
      sb.append("# Log events from the following Java class/package:\n");
      sb.append("log4j.logger.").append(packageName).append(" = INFO, flume\n");
    }

    output(sb.toString(), console, outputPath);

    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Print log4j configuration to log to dataset \"users\":",
        "--host flume.cluster.com --class org.kitesdk.examples.MyLoggingApp users",
        "# Save log4j configuration to the file \"log4j.properties\":",
        "--host flume.cluster.com --package org.kitesdk.examples -o log4j.properties users",
        "# Print log4j configuration to log from all classes:",
        "--host flume.cluster.com --log-all users"
    );
  }
}
