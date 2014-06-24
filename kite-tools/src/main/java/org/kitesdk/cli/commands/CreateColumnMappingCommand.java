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
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.avro.Schema;
import org.kitesdk.data.ColumnMapping;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.ValidationException;
import org.slf4j.Logger;

@Parameters(commandDescription="Builds a partition strategy for a schema")
public class CreateColumnMappingCommand extends BaseCommand {

  private static final Pattern KEY_MAPPING = Pattern.compile("(\\w+):key");
  private static final Pattern VERSION_MAPPING = Pattern.compile("(\\w+):version");
  private static final Pattern FIELD_MAPPING = Pattern.compile("(\\w+):(\\w+)(?::(\\w+))?");

  private static final Set<Schema.Type> counterTypes = Sets
      .newHashSet(Schema.Type.INT, Schema.Type.LONG);
  private static final Set<Schema.Type> kacTypes = Sets
      .newHashSet(Schema.Type.RECORD, Schema.Type.MAP);

  private final Logger console;

  @Parameter(description="<field:type pairs>")
  List<String> partitions;

  @Parameter(names = {"-s", "--schema"}, required=true,
      description = "The file containing the Avro schema.")
  String avroSchemaFile;

  @Parameter(names = {"-p", "--partition-by"}, required=true,
      description = "The file containing the JSON partition strategy.")
  String partitionStrategyFile;

  @Parameter(names="--minimize",
      description="Minimize output size by eliminating white space")
  boolean minimize=false;

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="UWF_NULL_FIELD",
      justification = "Field set by JCommander")
  @Parameter(names={"-o", "--output"}, description="Save schema avsc to path")
  String outputPath = null;

  public CreateColumnMappingCommand(Logger console) {
    this.console = console;
  }

  @Override
  public int run() throws IOException {
    Schema schema = new Schema.Parser().parse(open(avroSchemaFile));

    boolean hasVersion = false;
    ColumnMapping.Builder mappingBuilder = new ColumnMapping.Builder();

    // handle version mappings first to know whether to use counters
    for (String mapping : partitions) {
      Matcher matcher = VERSION_MAPPING.matcher(mapping);
      if (matcher.matches()) {
        String source = matcher.group(1);
        ValidationException.check(schema.getField(source) != null,
            "Not a schema field: %s", source);
        mappingBuilder.occ(source);
        hasVersion = true;
      }
    }

    // add the non-version mappings
    for (String mapping : partitions) {
      Matcher matcher = KEY_MAPPING.matcher(mapping);
      if (matcher.matches()) {
        String source = matcher.group(1);
        ValidationException.check(schema.getField(source) != null,
            "Not a schema field: %s", source);
        mappingBuilder.key(source);
        continue;
      }
      matcher = VERSION_MAPPING.matcher(mapping);
      if (matcher.matches()) {
        continue; // already processed above
      }
      matcher = FIELD_MAPPING.matcher(mapping);
      if (matcher.matches()) {
        String source = matcher.group(1);
        String family = matcher.group(2);
        String qualOrPrefix = matcher.group(3);
        Schema.Field field = schema.getField(source);
        ValidationException.check(field != null,
            "Not a schema field: %s", source);
        ValidationException.check(family != null && !family.isEmpty(),
            "Missing column family: %s:?", source);
        Schema.Type type = schema.getField(source).schema().getType();
        if (!hasVersion && counterTypes.contains(type)) {
          if (qualOrPrefix == null) {
            mappingBuilder.counter(source, family, source);
          } else {
            mappingBuilder.counter(source, family, qualOrPrefix);
          }
        } else if (kacTypes.contains(type)) {
          mappingBuilder.keyAsColumn(source, family, qualOrPrefix);
        } else {
          if (qualOrPrefix == null) {
            mappingBuilder.column(source, family, source);
          } else {
            mappingBuilder.column(source, family, qualOrPrefix);
          }
        }
      } else {
        throw new ValidationException("Unknown mapping: " + mapping);
      }
    }

    // building the descriptor validates the schema and strategy
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .columnMapping(mappingBuilder.build())
        .schema(open(avroSchemaFile))
        .partitionStrategy(open(partitionStrategyFile))
        .build();

    String mapping = descriptor.getColumnMapping().toString(!minimize);

    output(mapping, console, outputPath);

    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# store email in the key, other fields in column family \"u\":",
        "email:key username:u id:u --schema user.avsc -o user-cols.json",
        "# store preferences hash-map in column family \"prefs\":",
        "... preferences:prefs --schema user.avsc",
        "# use the \"version\" field as an OCC version:",
        "... version:version --schema user.avsc"
    );
  }

}
