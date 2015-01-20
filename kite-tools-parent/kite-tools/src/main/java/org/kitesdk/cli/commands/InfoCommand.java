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
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.spi.ColumnMappingParser;
import org.kitesdk.data.spi.PartitionStrategyParser;
import org.slf4j.Logger;

@Parameters(commandDescription="Print all metadata for a Dataset")
public class InfoCommand extends BaseDatasetCommand {

  private static final Splitter NEWLINE = Splitter.on('\n');
  private static final Joiner NEWLINE_TAB = Joiner.on("\n\t\t");

  @Parameter(description = "<dataset name>")
  List<String> datasets;

  public InfoCommand(Logger console) {
    super(console);
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NP_NULL_ON_SOME_PATH",
      justification="Null case checked by precondition")
  public int run() throws IOException {
    Preconditions.checkArgument(
        datasets != null && !datasets.isEmpty(),
        "Missing dataset name");

    for (String name : datasets) {
      printInfo(console, load(name, GenericRecord.class).getDataset());
    }

    return 0;
  }

  private static void printInfo(Logger console, Dataset<?> dataset) {
    DatasetDescriptor desc = dataset.getDescriptor();
    String schema = ColumnMappingParser.removeEmbeddedMapping(
        PartitionStrategyParser.removeEmbeddedStrategy(desc.getSchema()))
        .toString(true);
    Collection<String> properties = desc.listProperties();

    console.info("\nDataset \"{}\":", dataset.getName());
    console.info("\tURI: \"{}\"", dataset.getUri());
    console.info("\tSchema: {}", indent(schema));
    if (desc.isPartitioned()) {
      console.info("\tPartition strategy: {}",
          indent(desc.getPartitionStrategy().toString(true)));
    } else {
      console.info("\tNot partitioned");
    }
    if (desc.isColumnMapped()) {
      console.info("\tColumn mapping: {}",
          indent(desc.getColumnMapping().toString(true)));
    }
    if (!properties.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      for (String prop : properties) {
        sb.append("\n\t\t").append(prop).append("=")
            .append(desc.getProperty(prop));
      }
      console.info("\tProperties:{}", sb.toString());
    }
  }

  private static String indent(String lines) {
    return NEWLINE_TAB.join(NEWLINE.split(lines));
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Print metadata about dataset \"users\":",
        "users"
    );
  }
}
