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
package org.kitesdk.maven.plugins;

import com.google.common.base.Preconditions;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.spi.DatasetRepository;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delete a dataset and its metadata.
 */
@Mojo(name = "delete-dataset", requiresProject = false)
public class DeleteDatasetMojo extends AbstractDatasetMojo {

  private static final Logger LOG = LoggerFactory.getLogger(DeleteDatasetMojo.class);

  /**
   * The name of the dataset to delete. Ignored if kite.uri is set.
   */
  @Parameter(property = "kite.datasetNamespace", defaultValue = "default")
  private String datasetNamespace;

  /**
   * The name of the dataset to delete. Ignored if kite.uri is set.
   */
  @Parameter(property = "kite.datasetName")
  private String datasetName;

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    if (uri != null) {
      Datasets.delete(uri);
    } else {
      LOG.warn(
          "kite.datasetName is deprecated, instead use kite.uri=<dataset-uri>");
      Preconditions.checkArgument(datasetName != null,
          "kite.datasetName is required if kite.uri is not used");
      DatasetRepository repo = getDatasetRepository();
      repo.delete(datasetNamespace, datasetName);
    }
  }
}
