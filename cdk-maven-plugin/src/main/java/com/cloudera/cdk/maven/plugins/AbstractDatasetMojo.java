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
package com.cloudera.cdk.maven.plugins;

import com.cloudera.cdk.data.DatasetRepositories;
import com.cloudera.cdk.data.DatasetRepository;
import com.cloudera.cdk.data.filesystem.FileSystemDatasetRepository;
import com.cloudera.cdk.data.hcatalog.HCatalogDatasetRepository;
import com.google.common.annotations.VisibleForTesting;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.maven.plugins.annotations.Parameter;

abstract class AbstractDatasetMojo extends AbstractHadoopMojo {

  /**
   * The root directory of the dataset repository. Optional if using HCatalog for metadata storage.
   */
  @Parameter(property = "cdk.rootDirectory")
  protected String rootDirectory;

  /**
   * If true, store dataset metadata in HCatalog, otherwise store it on the filesystem.
   */
  @Parameter(property = "cdk.hcatalog")
  protected boolean hcatalog = true;

  /**
   * The URI specifying the dataset repository, e.g. <i>repo:hdfs://host:8020/data</i>.
   * Optional, but if specified then <code>cdk.rootDirectory</code> and
   * <code>cdk.hcatalog</code> are ignored.
   */
  @Parameter(property = "cdk.repositoryUri")
  protected String repositoryUri;

  /**
   * Hadoop configuration properties.
   */
  @Parameter(property = "cdk.hadoopConfiguration")
  private Properties hadoopConfiguration;

  @VisibleForTesting
  FileSystemDatasetRepository.Builder fsRepoBuilder = new
      FileSystemDatasetRepository.Builder();

  @VisibleForTesting
  HCatalogDatasetRepository.Builder hcatRepoBuilder = new
      HCatalogDatasetRepository.Builder();

  private Configuration getConf() {
    Configuration conf = new Configuration(false);
    for (String key : hadoopConfiguration.stringPropertyNames()) {
      String value = hadoopConfiguration.getProperty(key);
      conf.set(key, value);
    }
    return conf;
  }

  DatasetRepository getDatasetRepository() {
    DatasetRepository repo;
    if (repositoryUri != null) {
      return DatasetRepositories.open(repositoryUri);
    }
    if (!hcatalog && rootDirectory == null) {
      throw new IllegalArgumentException("Root directory must be specified if not " +
          "using HCatalog.");
    }
    if (rootDirectory != null) {
      try {
        URI uri = new URI(rootDirectory);
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
      repo = fsRepoBuilder.configuration(getConf()).build();
    }
    return repo;
  }
}
