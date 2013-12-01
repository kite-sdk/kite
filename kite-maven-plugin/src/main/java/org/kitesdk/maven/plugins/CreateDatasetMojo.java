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

import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.Formats;
import org.kitesdk.data.impl.Accessor;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;

/**
 * Create a named dataset whose entries conform to a defined schema.
 */
@Mojo(name = "create-dataset", requiresProject = false, requiresDependencyResolution = ResolutionScope.COMPILE)
public class CreateDatasetMojo extends AbstractDatasetMojo {

  /**
   * The name of the dataset to create.
   */
  @Parameter(property = "cdk.datasetName", required = true)
  private String datasetName;

  /**
   * The file containing the Avro schema. If no file with the specified name is found
   * on the local filesystem, then the classpath is searched for a matching resource.
   * One of either this property or <code>cdk.avroSchemaReflectClass</code> must be
   * specified.
   */
  @Parameter(property = "cdk.avroSchemaFile")
  private String avroSchemaFile;

  /**
   * The fully-qualified classname of the Avro reflect class to use to generate a
   * schema. The class must be available on the classpath.
   * One of either this property or <code>cdk.avroSchemaFile</code> must be
   * specified.
   */
  @Parameter(property = "cdk.avroSchemaReflectClass")
  private String avroSchemaReflectClass;

  /**
   * The file format (avro or parquet).
   */
  @Parameter(property = "cdk.format")
  private String format = Formats.AVRO.getName();

  /**
   * The partition expression, in JEXL format (experimental).
   */
  @Parameter(property = "cdk.partitionExpression")
  private String partitionExpression;

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    if (avroSchemaFile == null && avroSchemaReflectClass == null) {
      throw new IllegalArgumentException("One of cdk.avroSchemaFile or " +
          "cdk.avroSchemaReflectClass must be specified");
    }

    DatasetRepository repo = getDatasetRepository();

    DatasetDescriptor.Builder descriptorBuilder = new DatasetDescriptor.Builder();
    configureSchema(descriptorBuilder, avroSchemaFile, avroSchemaReflectClass);

    if (format.equals(Formats.AVRO.getName())) {
      descriptorBuilder.format(Formats.AVRO);
    } else if (format.equals(Formats.PARQUET.getName())) {
      descriptorBuilder.format(Formats.PARQUET);
    } else {
      throw new MojoExecutionException("Unrecognized format: " + format);
    }

    if (partitionExpression != null) {
      descriptorBuilder.partitionStrategy(Accessor.getDefault().fromExpression(partitionExpression));
    }

    repo.create(datasetName, descriptorBuilder.build());
  }

}
