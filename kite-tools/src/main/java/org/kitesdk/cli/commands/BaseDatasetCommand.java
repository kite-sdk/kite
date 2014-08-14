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
import com.beust.jcommander.internal.Lists;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.util.List;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.DatasetRepositories;
import org.kitesdk.data.spi.DatasetRepository;
import org.slf4j.Logger;

abstract class BaseDatasetCommand extends BaseCommand {

  @Parameter(names = {"-d", "--directory"}, hidden = true,
      description = "Storage directory for datasets, required for HDFS")
  String directory = null;

  @Parameter(names = {"--namespace"}, hidden = true,
      description = "Namespace for datasets")
  String namespace = "default";

  @Parameter(names = {"--use-local"}, hidden = true,
      description = "Store data in local files")
  boolean local = false;

  @Parameter(names = {"--use-hdfs"}, hidden = true,
      description = "Store data in HDFS files")
  boolean hdfs = false;

  @Parameter(names = {"--use-hive"}, hidden = true,
      description = "Store data in Hive managed tables (default)")
  boolean hive = false;

  @Parameter(names = {"--use-hbase"}, hidden = true,
      description = "Store data in HBase tables")
  boolean hbase = false;

  @Parameter(names = {"--zookeeper", "--zk"}, hidden = true,
      description = "ZooKeeper host list as host or host:port")
  List<String> zookeeper = Lists.newArrayList("localhost");

  @VisibleForTesting
  @Parameter(names = {"-r", "--repo"}, hidden = true,
      description="The repository URI to open")
  String repoURI = null;

  protected final Logger console;
  private DatasetRepository repo = null;

  public BaseDatasetCommand(Logger console) {
    this.console = console;
  }

  protected DatasetRepository getDatasetRepository() {
    if (repo == null) {
      this.repo = DatasetRepositories.repositoryFor(buildRepoURI());
    }
    return repo;
  }

  protected boolean isDataUri(String uriOrName) {
    return (uriOrName.startsWith("dataset:") || uriOrName.startsWith("view:"));
  }

  protected boolean isRepoUri(String uriOrName) {
    return uriOrName.startsWith("repo:");
  }

  protected <E> View<E> load(String uriOrName, Class<E> type) {
    if (isDataUri(uriOrName)) {
      return Datasets.<E, View<E>>load(uriOrName, type);
    } else {
      return getDatasetRepository().load(namespace, uriOrName);
    }
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
      Preconditions.checkArgument(!(hdfs || hive || hbase),
          "Only one storage implementation can be selected");
      Preconditions.checkArgument(directory != null,
          "--directory is required when using local files");
      uri = "repo:file:" + directory;
    } else if (hdfs) {
      Preconditions.checkArgument(!(hive || hbase),
          "Only one storage implementation can be selected");
      Preconditions.checkArgument(directory != null,
          "--directory is required when using HDFS");
      uri = "repo:hdfs:" + directory;
    } else if (hbase) {
      Preconditions.checkArgument(!hive,
          "Only one storage implementation can be selected");
      Preconditions.checkArgument(zookeeper != null && !zookeeper.isEmpty(),
          "--zookeeper is required when using HBase");
      uri = "repo:hbase:" + Joiner.on(",").join(zookeeper);
    } else {
      uri = "repo:hive" + (directory != null ? ":" + directory : "");
    }
    console.trace("Repository URI: " + uri);
    return uri;
  }

}
