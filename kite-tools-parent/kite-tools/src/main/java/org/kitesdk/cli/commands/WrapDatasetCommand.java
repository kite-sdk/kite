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

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.URIBuilder;
import org.kitesdk.data.ValidationException;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.Pair;
import org.kitesdk.data.spi.Registration;
import org.kitesdk.data.spi.filesystem.FileSystemDatasetRepository;
import org.kitesdk.data.spi.filesystem.FileSystemUtil;
import org.slf4j.Logger;

@Parameters(commandDescription = "Turn directories of data into a dataset")
public class WrapDatasetCommand extends BaseDatasetCommand {

  @Parameter(description = "<dataset>")
  List<String> datasets;

  @Parameter(names = {"-s", "--schema"},
      description = "A file containing an Avro schema for the dataset.")
  String avroSchemaFile;

  @Parameter(names = {"--location"},
      description = "Location where the data is stored")
  String location;

  @Parameter(names = {"-p", "--partition-by"},
      description = "A file containing a JSON-formatted partition strategy.")
  String partitionStrategyFile;

  @Parameter(names = {"-f", "--format"},
      description = "The file format: avro or parquet.")
  String formatFromArgs = null;

  @DynamicParameter(names = {"--set", "--property"},
      description = "Add a property pair: prop.name=value")
  Map<String, String> properties;

  public WrapDatasetCommand(Logger console) {
    super(console);
  }

  @Override
  public int run() throws IOException {
    if (datasets == null || datasets.size() != 1) {
      throw new IllegalArgumentException(
          "Exactly one dataset name must be specified.");
    }

    String dataset = datasets.get(0);

    DatasetRepository repo;
    String namespace;
    String name;

    if (isDatasetOrViewUri(dataset)) {
      URI uri = URI.create(URI.create(dataset).getSchemeSpecificPart());
      Pair<DatasetRepository, Map<String, String>> reg = Registration
          .lookupDatasetUri(uri);
      repo = reg.first();
      namespace = reg.second().get(URIBuilder.NAMESPACE_OPTION);
      name = reg.second().get(URIBuilder.DATASET_NAME_OPTION);
      if (location == null) {
        location = reg.second().get("location");
      }

    } else {
      repo = getDatasetRepository();
      namespace = URIBuilder.NAMESPACE_DEFAULT;
      name = dataset;
    }

    if (!(repo instanceof FileSystemDatasetRepository)) {
      throw new IllegalArgumentException(
          "Cannot wrap " + dataset + ": not a file system URI");
    }

    Preconditions.checkArgument(repo.exists(namespace, name),
        "Cannot create " + dataset + ": already exists");

    DatasetDescriptor.Builder descriptorBuilder = new DatasetDescriptor.Builder();

    Path dataPath;
    if (location != null) {
      dataPath = qualifiedPath(location);
    } else {
      dataPath = ((FileSystemDatasetRepository) repo)
          .pathForDataset(namespace, name);
    }

    descriptorBuilder.location(dataPath);

    FileSystem fs = dataPath.getFileSystem(getConf());

    Format format = FileSystemUtil.format(fs, dataPath);
    if (format != null) {
      ValidationException.check(formatFromArgs == null ||
              Formats.fromString(formatFromArgs).equals(format),
          "Found %s data, but --format is %s",
          format.getName(), formatFromArgs);
      descriptorBuilder.format(format);
    } else if (formatFromArgs != null) {
      descriptorBuilder.format(formatFromArgs);
    } else {
      throw new ValidationException(
          "Cannot determine the data format: use --format to set one");
    }

    if (avroSchemaFile != null) {
      descriptorBuilder.schemaUri(qualifiedURI(avroSchemaFile));
    } else {
      descriptorBuilder.schema(FileSystemUtil.schema("record", fs, dataPath));
    }

    if (partitionStrategyFile != null) {
      descriptorBuilder.partitionStrategyUri(qualifiedURI(partitionStrategyFile));
    } else {
      descriptorBuilder.partitionStrategy(FileSystemUtil.strategy(fs, dataPath));
    }

    if (properties != null) {
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        descriptorBuilder.property(entry.getKey(), entry.getValue());
      }
    }

    repo.create(namespace, name, descriptorBuilder.build());

    console.info("Created {}", dataset);

    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Create dataset for data in hdfs:/data/example/movies:",
        "dataset:hdfs:/data/example/movies hdfs:/data/example/movies"
    );
  }

}
