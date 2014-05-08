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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.cli.Command;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.ValidationException;
import org.slf4j.Logger;

@Parameters(commandDescription="Builds a partition strategy for a schema")
public class CreatePartitionStrategyCommand implements Configurable, Command {

  @VisibleForTesting
  static final Charset CHARSET = Charset.forName("utf8");

  private static final Pattern PARTITION_FIELD = Pattern.compile(
      "(\\w+):(\\w+)(?:\\[(\\d+)\\])?");

  private final Logger console;
  private Configuration conf;

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

    // use local FS to make qualified paths rather than the default FS
    FileSystem localFS = FileSystem.getLocal(getConf());
    Path cwd = localFS.makeQualified(new Path("."));

    PartitionStrategy.Builder strategyBuilder = new PartitionStrategy.Builder();
    for (String partition : partitions) {
      Matcher m = PARTITION_FIELD.matcher(partition);
      if (m.matches()) {
        String fieldName = m.group(1);
        ValidationException.check(fieldName != null && !fieldName.isEmpty(),
            "Invalid field name: %s", String.valueOf(fieldName));
        if ("hash".equals(m.group(2))) {
          String width = m.group(3);
          ValidationException.check(width != null,
              "Missing number of hash partitions: %s:hash[?]", fieldName);
          strategyBuilder.hash(fieldName, Integer.valueOf(width));
        } else if ("copy".equals(m.group(2))) {
          strategyBuilder.identity(fieldName);
        } else if ("year".equals(m.group(2))) {
          strategyBuilder.year(fieldName);
        } else if ("month".equals(m.group(2))) {
          strategyBuilder.month(fieldName);
        } else if ("day".equals(m.group(2))) {
          strategyBuilder.day(fieldName);
        } else if ("hour".equals(m.group(2))) {
          strategyBuilder.hour(fieldName);
        } else if ("minute".equals(m.group(2))) {
          strategyBuilder.minute(fieldName);
        } else {
          throw new ValidationException(
              "Unknown partitioner type: " + m.group(2));
        }
      } else {
        throw new ValidationException(
            "Invalid partition <field:type>: " + partition);
      }
    }

    Path schemaPath = new Path(avroSchemaFile)
        .makeQualified(localFS.getUri(), cwd);
    // even though it was qualified using the local FS, it may not be local
    FileSystem schemaFS = schemaPath.getFileSystem(getConf());

    DatasetDescriptor.Builder descBuilder = new DatasetDescriptor.Builder();
    descBuilder.partitionStrategy(strategyBuilder.build());
    if (schemaFS.exists(schemaPath)) {
      descBuilder.schema(schemaFS.open(schemaPath));
    } else {
      descBuilder.schema(Resources.getResource(avroSchemaFile).openStream());
    }

    // building the descriptor validates the schema and strategy
    DatasetDescriptor descriptor = descBuilder.build();
    String strategy = descriptor.getPartitionStrategy().toString(!minimize);

    if (outputPath == null || "-".equals(outputPath)) {
      console.info(strategy);
    } else {
      Path out = new Path(outputPath).makeQualified(localFS.getUri(), cwd);
      FileSystem outFS = out.getFileSystem(conf);
      FSDataOutputStream outgoing = outFS.create(out, true /* overwrite */ );
      try {
        outgoing.write(strategy.getBytes(CHARSET));
      } finally {
        outgoing.close();
      }
    }

    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Partition by email address, balanced across 16 hash partitions",
        "email:hash(16) email:copy -s user.avsc -o part.avsc",
        "# Partition by created_at time's year, month, and day",
        "created_at:year created_at:month created_at:day -s event.avsc"
    );
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

}
