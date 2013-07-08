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
package com.cloudera.cdk.tools.commands;

import com.beust.jcommander.Parameter;
import com.cloudera.cdk.data.DatasetRepository;
import com.cloudera.cdk.data.filesystem.FileSystemDatasetRepository;
import com.cloudera.cdk.data.hcatalog.HCatalogDatasetRepository;
import com.cloudera.cdk.tools.Command;
import com.google.common.annotations.VisibleForTesting;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configured;

abstract class BaseDatasetCommand extends Configured implements Command {
  @Parameter(names = {"-d", "--directory"},
      description = "The root directory of the dataset repository. Optional if using " +
          "HCatalog for metadata storage.")
  String directory;

  @Parameter(names = {"-h", "--hcatalog"},
      description = "If true, store dataset metadata in HCatalog, " +
          "otherwise store it on the filesystem.")
  boolean hcatalog = true;

  @VisibleForTesting
  FileSystemDatasetRepository.Builder fsRepoBuilder = new
      FileSystemDatasetRepository.Builder();

  @VisibleForTesting
  HCatalogDatasetRepository.Builder hcatRepoBuilder = new
      HCatalogDatasetRepository.Builder();

  DatasetRepository getDatasetRepository() {
    DatasetRepository repo;
    if (!hcatalog && directory == null) {
      throw new IllegalArgumentException("Root directory must be specified if not " +
          "using HCatalog.");
    }
    if (directory != null) {
      try {
        URI uri = new URI(directory);
        if (hcatalog) {
          hcatRepoBuilder.rootDirectory(uri);
        } else {
          fsRepoBuilder.rootDirectory(uri);
        }
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e);
      }
    }
    if (hcatalog) {
      repo = hcatRepoBuilder.configuration(getConf()).get();
    } else {
      repo = fsRepoBuilder.configuration(getConf()).get();
    }
    return repo;
  }
}
