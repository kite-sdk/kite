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
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.crunch.DoFn;
import org.apache.crunch.PipelineResult;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.compat.DynConstructors;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.ColumnMappingParser;
import org.kitesdk.data.spi.PartitionStrategyParser;
import org.kitesdk.data.spi.filesystem.CSVProperties;
import org.kitesdk.data.spi.filesystem.FileSystemDataset;
import org.kitesdk.data.spi.filesystem.TemporaryFileSystemDatasetRepository;
import org.kitesdk.tools.CopyTask;
import org.kitesdk.tools.TaskUtil;
import org.kitesdk.tools.TransformTask;
import org.slf4j.Logger;

import static org.apache.avro.generic.GenericData.Record;

@Parameters(commandDescription="Copy CSV records into a Dataset")
public class CSVImportCommand extends BaseDatasetCommand {

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

  @Parameter(names="--header",
      description="Line to use as a header. Must match the CSV settings.")
  String header;

  @Parameter(names={"--no-compaction"},
      description="Copy to output directly, without compacting the data")
  boolean noCompaction = false;

  @Parameter(names={"--num-writers"},
      description="The number of writer processes to use")
  int numWriters = -1;

  @Parameter(names={"--transform"},
      description="A transform DoFn class name")
  String transform = null;

  @Parameter(names="--jar",
      description="Add a jar to the runtime classpath")
  List<String> jars;

  @Override
  @SuppressWarnings("unchecked")
  public int run() throws IOException {
    Preconditions.checkArgument(targets != null && targets.size() == 2,
        "CSV path and target dataset name are required.");

    Path source = qualifiedPath(targets.get(0));
    FileSystem sourceFS = source.getFileSystem(getConf());
    Preconditions.checkArgument(sourceFS.exists(source),
        "CSV path does not exist: " + source);

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

    String dataset = targets.get(1);

    View<Record> target = load(dataset, Record.class);
    Schema datasetSchema = target.getDataset().getDescriptor().getSchema();

    // TODO: replace this with a temporary Dataset from a FS repo
    // TODO: CDK-92: always use GenericRecord?

    DatasetDescriptor csvDescriptor = new DatasetDescriptor.Builder()
        .location(source.toUri())
        .schema(ColumnMappingParser.removeEmbeddedMapping(
            PartitionStrategyParser.removeEmbeddedStrategy(datasetSchema)))
        .format("csv")
        .build();
    csvDescriptor = props.addToDescriptor(csvDescriptor);

    TemporaryFileSystemDatasetRepository repo =
        new TemporaryFileSystemDatasetRepository(getConf(),
            // ensure the same FS as the file source is used
            sourceFS.makeQualified(new Path("/tmp/" + UUID.randomUUID().toString())),
            target.getDataset().getNamespace(),
            UUID.randomUUID().toString());

    try {
      FileSystemDataset<Record> csvDataset =
          (FileSystemDataset) repo.create("default", "csv", csvDescriptor);

      Iterator<Path> iter = csvDataset.pathIterator().iterator();
      Preconditions.checkArgument(iter.hasNext(),
          "CSV path has no data files: " + source);

      TaskUtil.configure(getConf()).addJars(jars);

      TransformTask task;
      if (transform != null) {
        DoFn<Record, Record> transformFn;
        try {
          DynConstructors.Ctor<DoFn<Record, Record>> ctor =
              new DynConstructors.Builder(DoFn.class)
                  .loader(loaderForJars(jars))
                  .impl(transform)
                  .buildChecked();
          transformFn = ctor.newInstance();
        } catch (NoSuchMethodException e) {
          throw new DatasetException(
              "Cannot find no-arg constructor for class: " + transform, e);
        }
        task = new TransformTask<Record, Record>(
            csvDataset, target, transformFn);
      } else {
        task = new CopyTask<Record>(csvDataset, target);
      }

      task.setConf(getConf());

      if (noCompaction) {
        task.noCompaction();
      }

      if (numWriters >= 0) {
        task.setNumWriters(numWriters);
      }

      PipelineResult result = task.run();

      if (result.succeeded()) {
        long count = task.getCount();
        if (count > 0) {
          console.info("Added {} records to \"{}\"", count, dataset);
        }
        return 0;
      } else {
        return 1;
      }
    } finally {
      // clean up the temporary repository
      repo.delete();
    }
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Copy the records from sample.csv to dataset \"sample\"",
        "csv-import path/to/sample.csv sample",
        "# Copy the records from sample.csv to a dataset URI",
        "csv-import path/to/sample.csv dataset:hdfs:/user/me/datasets/sample",
        "# Copy the records from an HDFS directory to \"sample\"",
        "csv-import hdfs:/data/path/samples/ sample"
    );
  }
}
