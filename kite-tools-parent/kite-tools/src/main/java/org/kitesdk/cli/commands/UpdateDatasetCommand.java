/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import com.google.common.base.Splitter;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;
import org.slf4j.Logger;

@Parameters(commandDescription = "Update the metadata descriptor for dataset")
public class UpdateDatasetCommand extends BaseDatasetCommand {

  private static final Splitter PROP_VALUE_SEP = Splitter.on('=').limit(2);

  @Parameter(description = "<dataset>")
  List<String> datasets;

  @Parameter(names = {"-s", "--schema"},
      description = "The file containing the Avro schema.")
  String avroSchemaFile = null;

  @Parameter(names = {"--set", "--property"},
      description = "Add a property pair: prop.name=value")
  List<String> properties;

  public UpdateDatasetCommand(Logger console) {
    super(console);
  }

  @Override
  public int run() throws IOException {
    if (datasets == null || datasets.size() != 1) {
      throw new IllegalArgumentException(
          "Exactly one dataset name must be specified.");
    }

    String dataset = datasets.remove(0);
    Dataset<GenericData.Record> currentDataset = load(dataset, GenericData.Record.class).getDataset();

    DatasetDescriptor.Builder descriptorBuilder = new DatasetDescriptor
        .Builder(currentDataset.getDescriptor());

    if (avroSchemaFile != null) {
      descriptorBuilder.schemaUri(qualifiedURI(avroSchemaFile));
    }

    if (properties != null) {
      for (String propValue : properties) {
        Iterator<String> parts = PROP_VALUE_SEP.split(propValue).iterator();
        descriptorBuilder.property(
            Iterators.getNext(parts, null),
            Iterators.getNext(parts, null));
      }
    }

    DatasetDescriptor descriptor = descriptorBuilder.build();

    if (isDataUri(dataset)) {
      Datasets.<GenericData.Record, Dataset<GenericData.Record>> update(dataset, descriptor, GenericData.Record.class);
    } else {
      getDatasetRepository().update(namespace, dataset, descriptor);
    }

    console.debug("Updated {}", dataset);

    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Update schema for dataset \"users\" in Hive:",
        "users --schema user.avsc",
        "# Update HDFS dataset by URI, add property:",
        "dataset:hdfs:/user/me/datasets/users --set kite.write.cache-size=20"
    );
  }

}
