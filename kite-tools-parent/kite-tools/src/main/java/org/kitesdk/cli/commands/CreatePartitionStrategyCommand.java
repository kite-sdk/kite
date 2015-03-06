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
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.ValidationException;
import org.slf4j.Logger;

@Parameters(commandDescription="Builds a partition strategy for a schema")
public class CreatePartitionStrategyCommand extends BaseCommand {

  private static final Pattern PARTITION_FIELD = Pattern.compile(
      "((?:\\w|\\.)+):(\\w+)(?:\\[(\\d+)\\])?");

  private final Logger console;

  @Parameter(description="<field:type pairs>")
  List<String> partitions;

  @Parameter(names = {"-s", "--schema"}, required=true,
      description = "The file containing the Avro schema.")
  String avroSchemaFile;

  @Parameter(names="--minimize",
      description="Minimize output size by eliminating white space")
  boolean minimize=false;

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="UWF_NULL_FIELD",
      justification = "Field set by JCommander")
  @Parameter(names={"-o", "--output"}, description="Save schema avsc to path")
  String outputPath = null;

  public CreatePartitionStrategyCommand(Logger console) {
    this.console = console;
  }

  @Override
  public int run() throws IOException {
    PartitionStrategy.Builder strategyBuilder = new PartitionStrategy.Builder();
    for (String partition : partitions) {
      Matcher m = PARTITION_FIELD.matcher(partition);
      if (m.matches()) {
        String fieldName = m.group(1);
        ValidationException.check(fieldName != null && !fieldName.isEmpty(),
            "Invalid field name: %s", String.valueOf(fieldName));
        String partitionerType = m.group(2);
        if ("hash".equals(partitionerType)) {
          String width = m.group(3);
          ValidationException.check(width != null,
              "Missing number of hash partitions: %s:hash[?]", fieldName);
          strategyBuilder.hash(fieldName, Integer.parseInt(width));
        } else if ("copy".equals(partitionerType)) {
          strategyBuilder.identity(fieldName);
        } else if ("year".equals(partitionerType)) {
          strategyBuilder.year(fieldName);
        } else if ("month".equals(partitionerType)) {
          strategyBuilder.month(fieldName);
        } else if ("day".equals(partitionerType)) {
          strategyBuilder.day(fieldName);
        } else if ("hour".equals(partitionerType)) {
          strategyBuilder.hour(fieldName);
        } else if ("minute".equals(partitionerType)) {
          strategyBuilder.minute(fieldName);
        } else if ("provided".equals(partitionerType)) {
          strategyBuilder.provided(fieldName);
        } else {
          throw new ValidationException(
              "Unknown partitioner type: " + partitionerType);
        }
      } else {
        throw new ValidationException(
            "Invalid partition <field:type>: " + partition);
      }
    }

    // building the descriptor validates the schema and strategy
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .partitionStrategy(strategyBuilder.build())
        .schema(open(avroSchemaFile))
        .build();

    String strategy = descriptor.getPartitionStrategy().toString(!minimize);

    output(strategy, console, outputPath);

    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Partition by email address, balanced across 16 hash partitions",
        "email:hash[16] email:copy -s user.avsc -o email-part.json",
        "# Partition by created_at time's year, month, and day",
        "created_at:year created_at:month created_at:day -s event.avsc"
    );
  }

}
