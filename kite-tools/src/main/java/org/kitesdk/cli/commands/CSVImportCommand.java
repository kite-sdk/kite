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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.spi.filesystem.CSVProperties;
import org.kitesdk.data.spi.filesystem.FileSystemDataset;
import org.slf4j.Logger;

@Parameters(commandDescription="Copy CSV records into a Dataset")
public class CSVImportCommand
    extends BaseDatasetCommand implements Configurable {

  private Configuration conf;

  public CSVImportCommand(Logger console) {
    super(console);
  }

  @Parameter(description="<csv path> <dataset name>")
  List<String> targets;

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
    Preconditions.checkArgument(targets != null && targets.size() == 2,
        "CSV path and target dataset name are required.");

    Path sourcePath = new Path(targets.get(0));
    FileSystem sourceFS = sourcePath.getFileSystem(conf);
    CSVProperties props = new CSVProperties.Builder()
        .delimiter(delimiter)
        .escape(escape)
        .quote(quote)
        .hasHeader(!noHeader)
        .linesToSkip(linesToSkip)
        .charset(charsetName)
        .build();

    String datasetName = targets.get(1);

    DatasetRepository targetRepo = getDatasetRepository();
    Dataset<Object> target = targetRepo.load(datasetName);

    // TODO: verify compatibility of CSV
    // TODO: replace this with a temporary Dataset from a FS repo
    // TODO: CDK-92: always use GenericRecord?

    FileSystemDataset<Object> csvSourceAsDataset = new FileSystemDataset.Builder()
        .name("temporary")
        .configuration(conf)
        .descriptor(props.addToDescriptor(new DatasetDescriptor.Builder()
            .schema(target.getDescriptor().getSchema())
            .location(sourceFS.makeQualified(sourcePath))
            .format("csv")
            .build()))
        .build();

    console.debug("Temporary CSV dataset: " + csvSourceAsDataset);
    console.debug("Target dataset: " + target);

    boolean threw = true;
    DatasetReader<Object> reader = csvSourceAsDataset.newReader();
    DatasetWriter<Object> writer = target.newWriter();

    try {
      reader.open();
      writer.open();
      for (Object record : reader) {
        writer.write(record);
      }

      threw = false;

    } finally {
      boolean readerThrew = true;
      try {
        Closeables.close(reader, threw);
        readerThrew = false;
      } finally {
        Closeables.close(writer, threw || readerThrew);
      }
    }

    // in the future use: Jobs.copy(conf, csvSourceAsDataset, target) ? 0 : 1;
    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Copy the records from sample.csv to Dataset \"sample\"",
        "csv-import path/to/sample.csv sample"
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
