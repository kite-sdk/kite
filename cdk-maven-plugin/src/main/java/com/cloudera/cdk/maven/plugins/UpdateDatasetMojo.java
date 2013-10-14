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

import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.DatasetRepository;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Update a dataset's schema.
 */
@Mojo(name = "update-dataset", requiresProject = false)
public class UpdateDatasetMojo extends AbstractDatasetMojo {

  private static final Logger logger = LoggerFactory.getLogger(UpdateDatasetMojo.class);

  /**
   * The name of the dataset to update.
   */
  @Parameter(property = "cdk.datasetName", required = true)
  private String datasetName;

  /**
   * The file containing the Avro schema. If no file with the specified name is found
   * on the local filesystem, then the classpath is searched for a matching resource.
   */
  @Parameter(property = "cdk.avroSchemaFile", required = true)
  private String avroSchemaFile;

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    DatasetRepository repo = getDatasetRepository();

    DatasetDescriptor descriptor = repo.load(datasetName).getDescriptor();
    DatasetDescriptor.Builder descriptorBuilder =
        new DatasetDescriptor.Builder(descriptor);
    File avroSchema = new File(avroSchemaFile);
    try {
      if (avroSchema.exists()) {
        descriptorBuilder.schema(avroSchema);
      } else {
        descriptorBuilder.schema(Resources.getResource(avroSchemaFile).openStream());
      }
    } catch (IOException e) {
      throw new MojoExecutionException("Problem while reading file " + avroSchemaFile, e);
    }

    repo.update(datasetName, descriptorBuilder.get());
  }
}
