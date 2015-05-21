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
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.URIBuilder;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.DatasetRepositories;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.Registration;
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

  protected static Map<String, String> optionsForUri(URI uri) {
    Preconditions.checkArgument(isDatasetOrViewUri(uri.toString()),
        "Must be a dataset or view URI: " + uri);
    return Registration.lookupDatasetUri(
        URI.create(uri.getSchemeSpecificPart())).second();
  }

  protected static boolean isViewUri(String uriOrName) {
    return uriOrName.startsWith("view:");
  }

  protected static boolean isDatasetUri(String uriOrName) {
    return uriOrName.startsWith("dataset:");
  }

  protected static boolean isDatasetOrViewUri(String uriOrName) {
    return (isDatasetUri(uriOrName) || isViewUri(uriOrName));
  }

  protected static boolean isRepoUri(String uriOrName) {
    return uriOrName.startsWith("repo:");
  }

  protected <E> View<E> load(String uriOrName, Class<E> type) {
    if (isDatasetOrViewUri(uriOrName)) {
      return Datasets.<E, View<E>>load(uriOrName, type);
    } else {
      return getDatasetRepository().load(namespace, uriOrName, type);
    }
  }

  protected View<GenericRecord> load(String uriOrName) {
    if (isDatasetOrViewUri(uriOrName)) {
      return Datasets.load(uriOrName);
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

  String buildDatasetUri(String uriOrName) {
    if (isDatasetOrViewUri(uriOrName)) {
      return uriOrName;
    }
    return new URIBuilder(buildRepoURI(), namespace, uriOrName).build().toString();
  }

  /**
   * Verify that a view matches the URI that loaded it without extra options.
   * <p>
   * This is used to prevent mis-interpreted URIs from succeeding. For example,
   * the URI: view:file:./table?year=2014&month=3&dy=14 resolves a view for
   * all of March 2014 because "dy" wasn't recognized as "day" and was ignored.
   * This works by verifying that all of the options are accounted for in the
   * final view and would fail the above because "dy" is not in the view's
   * options.
   *
   * @param view a View's URI
   * @param requested the requested View URI
   * @return true if the view's URI and the requested URI match exactly
   */
  @VisibleForTesting
  static boolean viewMatches(URI view, String requested) {
    // test that the requested options are a subset of the final options
    Map<String, String> requestedOptions = optionsForUri(URI.create(requested));
    Map<String, String> finalOptions = optionsForUri(view);
    for (Map.Entry<String, String> entry : requestedOptions.entrySet()) {
      if (!finalOptions.containsKey(entry.getKey()) ||
          !finalOptions.get(entry.getKey()).equals(entry.getValue())) {
        return false;
      }
    }
    return true;
  }
}
