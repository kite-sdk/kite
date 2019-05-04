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

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.internal.Lists;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.kitesdk.data.spi.filesystem.CSVProperties;
import org.kitesdk.data.spi.filesystem.CSVUtil;
import org.slf4j.Logger;

@Parameters(commandDescription="Build a schema from a CSV data sample")
public class CSVSchemaCommand extends BaseCommand {

  @VisibleForTesting
  static final Charset SCHEMA_CHARSET = Charset.forName("utf8");

  private final Logger console;
  private Configuration conf;

  public CSVSchemaCommand(Logger console) {
    this.console = console;
  }

  @Parameter(description="<sample csv path>")
  List<String> samplePaths;

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="UWF_NULL_FIELD",
      justification = "Field set by JCommander")
  @Parameter(names={"-o", "--output"}, description="Save schema avsc to path")
  String outputPath = null;

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="UWF_NULL_FIELD",
      justification = "Field set by JCommander")
  @Parameter(names={"--class", "--record-name"}, required = true,
      description="A name or class for the result schema")
  String recordName = null;

  @Parameter(names="--minimize",
      description="Minimize schema file size by eliminating white space")
  boolean minimize=false;

  @Parameter(names="--delimiter", description="Delimiter character")
  String delimiter = ",";

  @Parameter(names="--escape", description="Escape character")
  String escape = "\\";

  @Parameter(names="--quote", description="Quote character")
  String quote = "\"";

  @Parameter(names="--no-header", description="Don't use first line as CSV header")
  boolean noHeader = false;

  @Parameter(names="--skip-lines", description="Lines to skip before CSV start")
  int linesToSkip = 0;

  @Parameter(names="--charset", description="Character set name", hidden = true)
  String charsetName = Charset.defaultCharset().displayName();

  @Parameter(names="--header",
      description="Line to use as a header. Must match the CSV settings.")
  String header;

  @Parameter(names="--require",
      description="Do not allow null values for the given field")
  List<String> requiredFields;

  @DynamicParameter(names="--field-type",
      description="Specify the type for a field. Format is field_name=field_type. Valid types are string, int, boolean, long, float, and double.")
  Map<String,String> fieldTypes = new HashMap<String,String>();

  @Override
  public int run() throws IOException {
    Preconditions.checkArgument(samplePaths != null && !samplePaths.isEmpty(),
        "Sample CSV path is required");
    Preconditions.checkArgument(samplePaths.size() == 1,
        "Only one CSV sample can be given");

    if (header != null) {
      // if a header is given on the command line, do assume one is in the file
      noHeader = true;
    }

    CSVProperties props = new CSVProperties.Builder()
        .delimiter(delimiter)
        .escape(escape)
        .quote(quote)
        .header(header)
        .hasHeader(!noHeader)
        .linesToSkip(linesToSkip)
        .charset(charsetName)
        .build();

    Set<String> required = ImmutableSet.of();
    if (requiredFields != null) {
      required = ImmutableSet.copyOf(requiredFields);
    }

    // assume fields are nullable by default, users can easily change this
    String sampleSchema = CSVUtil
        .inferNullableSchema(
            recordName, open(samplePaths.get(0)), props, required, fieldTypes)
        .toString(!minimize);

    output(sampleSchema, console, outputPath);

    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Print the schema for samples.csv to standard out:",
        "samples.csv --record-name Sample",
        "# Write schema to sample.avsc:",
        "samples.csv -o sample.avsc --record-name Sample"
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