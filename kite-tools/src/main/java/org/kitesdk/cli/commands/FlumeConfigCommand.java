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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.spi.DatasetRepositories;
import org.slf4j.Logger;

@Parameters(commandDescription = "Build a Flume config to log events to a dataset")
public class FlumeConfigCommand extends BaseDatasetCommand {

  @Parameter(description = "<dataset name>", required = true)
  List<String> datasetName;

  @Parameter(names={"--use-dataset-uri"}, description = "Configure Flume with a dataset URI. Requires Flume 1.6+ or CDH5.2+")
  boolean newFlume;

  @Parameter(names={"--agent"}, description = "<flume agent name>")
  String agent = "tier1";

  @Parameter(names={"--source-name"}, description = "<source name>")
  String sourceName = "avro-log-source";

  @Parameter(names={"--bind"}, description = "<avro source bind address>")
  String bindAddress = "0.0.0.0";

  @Parameter(names={"-p", "--port"}, description = "<avro source port>")
  int port = 41415;

  @Parameter(names={"--channel-name"}, description = "<channel name>")
  String channelName = "channel-1";

  @Parameter(names={"--channel-type"}, description = "<channel type>")
  String channelType = "file";

  @Parameter(names={"--channel-capacity"}, description = "<channel capacity>")
  Integer capacity = null;
  int defaultMemoryChannelCapacity = 10000000;

  @Parameter(names={"--channel-transaction-capacity"}, description = "<channel transaction capacity>")
  Integer transactionCapacity = null;
  int defaultMemoryChannelTransactionCapacity = 1000;

  @Parameter(names={"--checkpoint-dir"}, description = "<file channel checkpoint directory>")
  String checkpointDir = null;

  @Parameter(names={"--data-dir"}, description = "<file channel data directory>")
  List<String> dataDirs = null;

  @Parameter(names={"--sink-name"}, description = "<sink name>")
  String sinkName = "kite-dataset";

  @Parameter(names={"--batch-size"}, description = "<records per batch>")
  int batchSize = 1000;

  @Parameter(names={"--roll-interval"}, description = "<wait time in seconds>")
  int rollInterval = 30;

  @Parameter(names={"--proxy-user"}, description = "<user to write to HDFS as>")
  String proxyUser = null;

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="UWF_NULL_FIELD",
      justification = "Field set by JCommander")
  @Parameter(names={"-o", "--output"}, description="Save logging config to path")
  String outputPath = null;

  public FlumeConfigCommand(Logger console) {
    super(console);
  }

  @Override
  public int run() throws IOException {
    Preconditions.checkArgument(
        datasetName != null && !datasetName.isEmpty(),
        "Missing dataset uri");

    Dataset<GenericRecord> dataset = load(datasetName.get(0), GenericRecord.class).getDataset();
    String datasetUri = buildDatasetUri(datasetName.get(0));
    URI repoUri = getRepoUri(dataset);
    String name = dataset.getName();

    StringBuilder sb = new StringBuilder();
    sb.append(agent).append(".sources = ").append(sourceName).append('\n');
    sb.append(agent).append(".channels = ").append(channelName).append('\n');
    sb.append(agent).append(".sinks = ").append(sinkName).append('\n');
    sb.append('\n');

    // Define Avro source
    sb.append(agent).append(".sources.").append(sourceName).append(".type = avro").append('\n');
    sb.append(agent).append(".sources.").append(sourceName).append(".channels = ").append(channelName).append('\n');
    sb.append(agent).append(".sources.").append(sourceName).append(".bind = ").append(bindAddress).append('\n');
    sb.append(agent).append(".sources.").append(sourceName).append(".port = ").append(port).append('\n');
    sb.append('\n');

    // Define channel
    sb.append(agent).append(".channels.").append(channelName).append(".type = ").append(channelType).append('\n');

    if ("memory".equals(channelType) && capacity == null) {
      capacity = defaultMemoryChannelCapacity;
    }

    if (capacity != null) {
      sb.append(agent).append(".channels.").append(channelName).append(".capacity = ").append(capacity).append('\n');
    }

    if ("memory".equals(channelType) && transactionCapacity == null) {
      transactionCapacity = defaultMemoryChannelTransactionCapacity;
    }

    if (transactionCapacity != null) {
      sb.append(agent).append(".channels.").append(channelName).append(".transactionCapacity = ").append(transactionCapacity).append('\n');
    }

    if ("file".equals(channelType) && checkpointDir != null) {
      sb.append(agent).append(".channels.").append(channelName).append(".checkpointDir = ").append(checkpointDir).append('\n');
    }

    if ("file".equals(channelType) && dataDirs != null && !dataDirs.isEmpty()) {
      sb.append(agent).append(".channels.").append(channelName).append(".dataDirs = ");
      boolean first = true;
      for (String dataDir : dataDirs) {
        if (!first) {
          sb.append(", ");
        }
        sb.append(dataDir);
        first = false;
      }
      sb.append('\n');
    }
    sb.append('\n');

    // Define Kite Dataset Sink
    sb.append(agent).append(".sinks.").append(sinkName).append(".type = org.apache.flume.sink.kite.DatasetSink").append('\n');
    sb.append(agent).append(".sinks.").append(sinkName).append(".channel = ").append(channelName).append('\n');
    if (newFlume) {
      sb.append(agent).append(".sinks.").append(sinkName).append(".kite.dataset.uri = ").append(datasetUri).append('\n');
    } else {
      sb.append(agent).append(".sinks.").append(sinkName).append(".kite.repo.uri = ").append(repoUri).append('\n');
      sb.append(agent).append(".sinks.").append(sinkName).append(".kite.dataset.name = ").append(name).append('\n');
    }
    sb.append(agent).append(".sinks.").append(sinkName).append(".kite.batchSize = ").append(batchSize).append('\n');
    sb.append(agent).append(".sinks.").append(sinkName).append(".kite.rollInterval = ").append(rollInterval).append('\n');
    if (proxyUser != null) {
      sb.append(agent).append(".sinks.").append(sinkName).append(".auth.proxyUser = ").append(proxyUser).append('\n');
    }

    output(sb.toString(), console, outputPath);

    return 0;
  }

  private URI getRepoUri(Dataset<GenericRecord> dataset) {
    return getRepoUri(dataset.getUri(), dataset.getNamespace());
  }

  @VisibleForTesting
  URI getRepoUri(URI datasetUri, String namespace) {
    URI repoUri = DatasetRepositories.repositoryFor(datasetUri).getUri();

    URI specificUri = URI.create(repoUri.getSchemeSpecificPart());
    String repoScheme = specificUri.getScheme();
    if (Sets.newHashSet("hdfs", "file", "hive").contains(repoScheme)) {
      try {
        specificUri = new URI(specificUri.getScheme(), specificUri.getUserInfo(),
            specificUri.getHost(), specificUri.getPort(),
            specificUri.getPath() + "/" + namespace,
            specificUri.getQuery(), specificUri.getFragment());
        repoUri = URI.create("repo:" + specificUri.toString());
      } catch (URISyntaxException ex) {
        throw new DatasetException("Error generating legacy URI", ex);
      }
    }

    return repoUri;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Print Flume configuration to log to dataset \"users\":",
        "--channel-type memory users",
        "# Save Flume configuration to the file \"flume.properties\":",
        "--channel-type memory -o flume.properties users"
    );
  }
}
