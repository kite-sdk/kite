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
import com.google.common.io.Resources;

import java.io.File;
import java.io.IOException;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Formats;
import org.kitesdk.data.impl.Accessor;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.PartitionStrategyParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create a named dataset whose entries conform to a defined schema.
 */
@Mojo(name = "create-dataset", requiresProject = false, requiresDependencyResolution = ResolutionScope.COMPILE)
public class CreateDatasetMojo extends AbstractDatasetMojo {

  private static final Logger LOG = LoggerFactory.getLogger(CreateDatasetMojo.class);

  /**
   * The name of the dataset to create. Ignored if kite.uri is set.
   */
  @Parameter(property = "kite.datasetNamespace", defaultValue = "default")
  private String datasetNamespace;

  /**
   * The name of the dataset to create. Ignored if kite.uri is set.
   */
  @Parameter(property = "kite.datasetName")
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

  /**
   * The file format (avro or parquet).
   */
  @Parameter(property = "kite.format")
  private String format = Formats.AVRO.getName();

  /**
   * The partition expression, in JEXL format (experimental).
   */
  @Parameter(property = "kite.partitionExpression")
  private String partitionExpression;

  @Parameter(property = "kite.partitionStrategyFile")
  private String partitionStrategyFile;

  @Parameter(property = "kite.columnDescriptorFile")
  private String columnDescriptorFile;

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    if (avroSchemaFile == null && avroSchemaReflectClass == null) {
      throw new IllegalArgumentException("One of kite.avroSchemaFile or "
          + "kite.avroSchemaReflectClass must be specified");
    }

    DatasetDescriptor.Builder descriptorBuilder = new DatasetDescriptor.Builder();
    configureSchema(descriptorBuilder, avroSchemaFile, avroSchemaReflectClass);

    if (format.equals(Formats.AVRO.getName())) {
      descriptorBuilder.format(Formats.AVRO);
    } else if (format.equals(Formats.PARQUET.getName())) {
      descriptorBuilder.format(Formats.PARQUET);
    } else {
      throw new MojoExecutionException("Unrecognized format: " + format);
    }

    if (partitionStrategyFile != null) {
      File partitionStrategy = new File(partitionStrategyFile);
      try {
        if (partitionStrategy.exists()) {
          descriptorBuilder.partitionStrategy(partitionStrategy);
        } else {
          descriptorBuilder.partitionStrategy(Resources.getResource(
              partitionStrategyFile).openStream());
        }
      } catch (IOException e) {
        throw new MojoExecutionException("Problem while reading file "
            + partitionStrategyFile, e);
      }
    } else if (partitionExpression != null) {
      descriptorBuilder.partitionStrategy(Accessor.getDefault().fromExpression(
          partitionExpression));
    }

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

    if (uri != null) {
      Datasets.create(uri, descriptorBuilder.build());
    } else {
      LOG.warn(
          "kite.datasetName is deprecated, instead use kite.uri=<dataset-uri>");
      Preconditions.checkArgument(datasetName != null,
          "kite.datasetName is required if kite.uri is not used");
      DatasetRepository repo = getDatasetRepository();
      repo.create(datasetNamespace, datasetName, descriptorBuilder.build());
    }
  }

}
