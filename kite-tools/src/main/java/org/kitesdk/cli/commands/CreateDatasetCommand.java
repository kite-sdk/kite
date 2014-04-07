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
import com.beust.jcommander.Parameters;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.Formats;
import org.slf4j.Logger;

@Parameters(commandDescription = "Create an empty dataset")
public class CreateDatasetCommand extends BaseDatasetCommand {

  @Parameter(description = "<dataset name>")
  List<String> datasetNames;

  @Parameter(names = {"-s", "--schema"}, required=true,
      description = "The file containing the Avro schema.")
  String avroSchemaFile;

  @Parameter(names = {"-f", "--format"},
      description = "The file format (avro or parquet).")
  String format = Formats.AVRO.getName();

  public CreateDatasetCommand(Logger console) {
    super(console);
  }

  @Override
  public int run() throws IOException {
    DatasetRepository repo = getDatasetRepository();

    DatasetDescriptor.Builder descriptorBuilder = new DatasetDescriptor.Builder();

    File avroSchema = new File(avroSchemaFile);
    if (avroSchema.exists()) {
      descriptorBuilder.schema(avroSchema);
    } else {
      descriptorBuilder.schema(Resources.getResource(avroSchemaFile).openStream());
    }

    if (format.equals(Formats.AVRO.getName())) {
      descriptorBuilder.format(Formats.AVRO);
    } else if (format.equals(Formats.PARQUET.getName())) {
      descriptorBuilder.format(Formats.PARQUET);
    } else {
      throw new IllegalArgumentException("Unrecognized format: " + format);
    }

    // TODO: Add partition strategy after format is finalized

    if (datasetNames == null || datasetNames.size() != 1) {
      throw new IllegalArgumentException(
          "Exactly one dataset name must be specified.");
    }

    repo.create(datasetNames.get(0), descriptorBuilder.build());
    console.debug("Created dataset {}", datasetNames.get(0));

    return 0;
  }

}
