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
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import org.kitesdk.data.Datasets;
import org.slf4j.Logger;

@Parameters(commandDescription="Find dataset URIs")
public class ListCommand extends BaseDatasetCommand {

  @Parameter(description = "[repository-uris]")
  List<String> repos;

  public ListCommand(Logger console) {
    super(console);
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="NP_NULL_ON_SOME_PATH",
      justification="Null case checked by precondition")
  public int run() throws IOException {
    if (repos == null || repos.isEmpty()) {
      printDatasetUris(console, getDatasetRepository().getUri());
    } else {
      for (String repo : repos) {
        printDatasetUris(console, URI.create(repo));
      }
    }

    return 0;
  }

  private static void printDatasetUris(Logger console, URI repoUri) {
    for (URI datasetUri : Datasets.list(repoUri)) {
      console.info(datasetUri.toString());
    }
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Print all Hive dataset URIs",
        "",
        "# Print all URIs in a HDFS repository",
        "repo:hdfs:/data"
    );
  }
}
