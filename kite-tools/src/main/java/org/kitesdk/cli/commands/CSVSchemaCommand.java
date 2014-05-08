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
import com.beust.jcommander.internal.Lists;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.cli.Command;
import org.kitesdk.data.spi.filesystem.CSVProperties;
import org.kitesdk.data.spi.filesystem.CSVUtil;
import org.slf4j.Logger;

@Parameters(commandDescription="Build a schema from a CSV data sample")
public class CSVSchemaCommand implements Configurable, Command {

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

  @Override
  public int run() throws IOException {
    Preconditions.checkArgument(samplePaths != null && !samplePaths.isEmpty(),
        "Sample CSV path is required");
    Preconditions.checkArgument(samplePaths.size() == 1,
        "Only one CSV sample can be given");

    // use local FS to make qualified paths rather than the default FS
    FileSystem localFS = FileSystem.getLocal(getConf());
    Path cwd = localFS.makeQualified(new Path("."));

    Path sample = new Path(samplePaths.get(0))
        .makeQualified(localFS.getUri(), cwd);
    FileSystem sampleFS = sample.getFileSystem(conf);

    CSVProperties props = new CSVProperties.Builder()
        .delimiter(delimiter)
        .escape(escape)
        .quote(quote)
        .hasHeader(!noHeader)
        .linesToSkip(linesToSkip)
        .charset(charsetName)
        .build();

    // assume fields are nullable by default, users can easily change this
    String sampleSchema = CSVUtil
        .inferNullableSchema(recordName, sampleFS.open(sample), props)
        .toString(!minimize);

    if (outputPath == null || "-".equals(outputPath)) {
      console.info(sampleSchema);
    } else {
      Path out = new Path(outputPath).makeQualified(localFS.getUri(), cwd);
      FileSystem outFS = out.getFileSystem(conf);
      FSDataOutputStream outgoing = outFS.create(out, true /* overwrite */ );
      try {
        outgoing.write(sampleSchema.getBytes(SCHEMA_CHARSET));
      } finally {
        outgoing.close();
      }
    }

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
