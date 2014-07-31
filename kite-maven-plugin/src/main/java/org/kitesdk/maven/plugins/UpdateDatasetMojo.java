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

import com.google.common.io.Resources;

import java.io.File;
import java.io.IOException;

import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.impl.Accessor;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Update a dataset's schema.
 */
@Mojo(name = "update-dataset", requiresProject = false, requiresDependencyResolution = ResolutionScope.COMPILE)
public class UpdateDatasetMojo extends AbstractDatasetMojo {

  private static final Logger LOG = LoggerFactory
      .getLogger(UpdateDatasetMojo.class);

  /**
   * The name of the dataset to update.
   */
  @Parameter(property = "kite.datasetName", required = true)
  private String datasetName;

  /**
   * The file containing the Avro schema. If no file with the specified name is
   * found on the local filesystem, then the classpath is searched for a
   * matching resource. One of either this property or
   * <code>kite.avroSchemaReflectClass</code> must be specified.
   */
  @Parameter(property = "kite.avroSchemaFile")
  private String avroSchemaFile;

  /**
   * The fully-qualified classname of the Avro reflect class to use to generate
   * a schema. The class must be available on the classpath. One of either this
   * property or <code>kite.avroSchemaFile</code> must be specified.
   */
  @Parameter(property = "kite.avroSchemaReflectClass")
  private String avroSchemaReflectClass;

  @Parameter(property = "kite.columnDescriptorFile")
  private String columnDescriptorFile;

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    if (avroSchemaFile == null && avroSchemaReflectClass == null) {
      throw new IllegalArgumentException("One of kite.avroSchemaFile or "
          + "kite.avroSchemaReflectClass must be specified");
    }

    DatasetRepository repo = getDatasetRepository();

    DatasetDescriptor descriptor = repo.load(datasetName).getDescriptor();
    DatasetDescriptor.Builder descriptorBuilder = new DatasetDescriptor.Builder(
        descriptor);
    configureSchema(descriptorBuilder, avroSchemaFile, avroSchemaReflectClass);

    if (columnDescriptorFile != null) {
      File columnDescriptor = new File(columnDescriptorFile);
      try {
        if (columnDescriptor.exists()) {
          descriptorBuilder.columnMapping(columnDescriptor);
        } else {
          descriptorBuilder.columnMapping(Resources.getResource(
              columnDescriptorFile).openStream());
        }
      } catch (IOException e) {
        throw new MojoExecutionException("Problem while reading file "
            + columnDescriptorFile, e);
      }
    }

    repo.update(datasetName, descriptorBuilder.build());
  }
}
