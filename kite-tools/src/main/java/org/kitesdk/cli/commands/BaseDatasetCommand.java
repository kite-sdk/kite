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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.kitesdk.cli.Command;
import org.kitesdk.data.DatasetRepositories;
import org.kitesdk.data.DatasetRepository;
import org.slf4j.Logger;

abstract class BaseDatasetCommand implements Command {
  @Parameter(names = {"-d", "--directory"},
      description = "The root directory of the dataset repository. Optional if using " +
          "HCatalog for metadata storage.")
  String directory = null;

  @Parameter(names = {"--use-hcatalog"}, arity = 1,
      description = "If true, store dataset metadata in HCatalog, " +
          "otherwise store it on the filesystem.")
  boolean hcatalog = true;

  @VisibleForTesting
  @Parameter(names = {"-r", "--repo"}, hidden = true,
      description="The repository URI to open")
  String repoURI = null;

  @Parameter(names = {"--local"}, hidden = true,
      description = "If set, use the local filesystem.")
  boolean local = false;

  protected final Logger console;

  public BaseDatasetCommand(Logger console) {
    this.console = console;
  }

  DatasetRepository getDatasetRepository() {
    return DatasetRepositories.open(buildRepoURI());
  }

  @VisibleForTesting
  public String buildRepoURI() {
    if (repoURI != null) {
      if (repoURI.startsWith("repo:")) {
        return repoURI;
      } else {
        return "repo:" + repoURI;
      }
    }
    String uri;
    if (local) {
      Preconditions.checkArgument(directory != null,
          "--directory is required when using --local");
      uri = "repo:file:" + directory;
    } else if (hcatalog) {
      uri = "repo:hive" + (directory != null ? ":" + directory : "");
    } else {
      Preconditions.checkArgument(directory != null,
          "--directory is required when not using Hive");
      uri = "repo:hdfs:" + directory;
    }
    console.trace("Repository URI: " + uri);
    return uri;
  }

}
