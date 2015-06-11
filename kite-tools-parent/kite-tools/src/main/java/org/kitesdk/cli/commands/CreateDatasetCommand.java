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
import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.URIBuilder;
import org.kitesdk.data.ValidationException;
import org.kitesdk.data.spi.Compatibility;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.Pair;
import org.kitesdk.data.spi.PartitionStrategyParser;
import org.kitesdk.data.spi.Registration;
import org.kitesdk.data.spi.SchemaValidationUtil;
import org.kitesdk.data.spi.Schemas;
import org.kitesdk.data.spi.filesystem.FileSystemDatasetRepository;
import org.kitesdk.data.spi.filesystem.FileSystemUtil;
import org.slf4j.Logger;

@Parameters(commandDescription = "Create an empty dataset")
public class CreateDatasetCommand extends BaseDatasetCommand {

  @Parameter(description = "<dataset name>")
  List<String> datasets;

  @Parameter(names = {"-s", "--schema"},
      description = "The file containing the Avro schema.")
  String avroSchemaFile;

  @Parameter(names = {"-p", "--partition-by"},
      description = "The file containing a JSON-formatted partition strategy.")
  String partitionStrategyFile;

  @Parameter(names = {"-m", "--mapping"},
      description = "The file containing a JSON-formatted column mapping.")
  String columnMappingFile;

  @Parameter(names = {"-f", "--format"},
      description = "The file format: avro, parquet, or json.")
  String format;

  @Parameter(names = {"--location"},
      description = "Location where the data is stored")
  String location;

  @DynamicParameter(names = {"--set", "--property"},
      description = "Add a property pair: prop.name=value")
  Map<String, String> properties = new HashMap<String, String>();

  public CreateDatasetCommand(Logger console) {
    super(console);
  }

  @Override
  public int run() throws IOException {
    if (datasets == null || datasets.size() != 1) {
      throw new IllegalArgumentException(
          "Exactly one dataset name or URI must be specified.");
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

    // for file-based datasets, try to infer schema, format, etc. from data
    boolean isFileDataset = (repo instanceof FileSystemDatasetRepository);

    DatasetDescriptor.Builder descriptorBuilder = new DatasetDescriptor.Builder();

    Path dataPath = null;
    if (location != null) {
      dataPath = qualifiedPath(location);
      descriptorBuilder.location(dataPath);
    } else if (isFileDataset) {
      dataPath = ((FileSystemDatasetRepository) repo)
          .pathForDataset(namespace, name);
    }

    FileSystem fs = isFileDataset ? dataPath.getFileSystem(getConf()) : null;

    Format existingFormat = null;
    if (isFileDataset) {
      existingFormat = FileSystemUtil.format(fs, dataPath);
    }

    if (existingFormat != null) {
      ValidationException.check(format == null ||
              Formats.fromString(format).equals(existingFormat),
          "Found %s data, but --format is %s",
          existingFormat.getName(), format);
      descriptorBuilder.format(existingFormat);

    } else if (format != null) {
      descriptorBuilder.format(format);
    }

    Schema existingSchema = null;
    if (isFileDataset) {
      existingSchema = FileSystemUtil.schema("record", fs, dataPath);
    }

    Schema schema = existingSchema;
    if (existingSchema != null) {
      if (avroSchemaFile != null) {
        schema = Schemas.fromAvsc(open(avroSchemaFile));
        ValidationException.check(
            SchemaValidationUtil.canRead(existingSchema, schema),
            "Schema %s cannot read existing data schema: %s",
            avroSchemaFile, existingSchema.toString(true));
        descriptorBuilder.schemaUri(qualifiedURI(avroSchemaFile));
      } else {
        descriptorBuilder.schema(existingSchema);
      }

    } else if (avroSchemaFile != null) {
      descriptorBuilder.schemaUri(qualifiedURI(avroSchemaFile));

    } else {
      throw new ValidationException("Schema is missing: use --schema");
    }

    PartitionStrategy existingStrategy = null;
    if (isFileDataset && existingSchema != null) {
      // if there is existing data (there is a Schema), infer partitioning
      existingStrategy = FileSystemUtil.strategy(fs, dataPath);
    }

    if (existingStrategy != null) {
      if (partitionStrategyFile != null) {
        // both are defined, so check that the new strategy is compatible
        PartitionStrategy strategy = PartitionStrategyParser
            .parse(open(partitionStrategyFile));
        Compatibility.checkStrategyUpdate(existingStrategy, strategy, schema);
        descriptorBuilder.partitionStrategy(strategy);
      } else {
        descriptorBuilder.partitionStrategy(existingStrategy);
      }

    } else if (partitionStrategyFile != null) {
      descriptorBuilder.partitionStrategyUri(qualifiedURI(partitionStrategyFile));
    }

    if (columnMappingFile != null) {
      descriptorBuilder.columnMappingUri(qualifiedURI(columnMappingFile));
    }

    if (properties != null) {
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        descriptorBuilder.property(entry.getKey(), entry.getValue());
      }
    }

    repo.create(namespace, name, descriptorBuilder.build());

    console.debug("Created {}", dataset);

    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Create dataset \"users\" in Hive:",
        "users --schema user.avsc",
        "# Create HDFS dataset \"users\":",
        "dataset:hdfs:/user/me/datasets/users --schema user.avsc",
        "# Create dataset \"users\" using parquet:",
        "users --schema user.avsc --format parquet",
        "# Create dataset \"users\" partitioned by JSON configuration:",
        "users --schema user.avsc --partition-by email-part.json",
        "# Create HBase dataset \"users\":",
        "dataset:hbase:zk1,zk2/users --schema user.avsc \\\n" +
            "\t\t--partition-by email-part.json --mapping user-cols.json"
    );
  }

}
