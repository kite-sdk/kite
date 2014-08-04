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
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.PipelineResult;
import org.kitesdk.data.View;
import org.kitesdk.tools.CopyTask;
import org.slf4j.Logger;

@Parameters(commandDescription="Copy records from one Dataset to another")
public class CopyCommand extends BaseDatasetCommand {

  public CopyCommand(Logger console) {
    super(console);
  }

  @Parameter(description="<source dataset> <destination dataset>")
  List<String> datasets;

  @Parameter(names={"--no-compaction"},
      description="Copy to output directly, without compacting the data")
  boolean noCompaction = false;

  @Parameter(names={"--num-writers"},
      description="The number of writer processes to use")
  int numWriters = -1;

  @Override
  public int run() throws IOException {
    Preconditions.checkArgument(datasets != null && datasets.size() > 1,
        "Source and target datasets are required");
    Preconditions.checkArgument(datasets.size() == 2,
        "Cannot copy multiple datasets");

    View<GenericData.Record> source = load(datasets.get(0), GenericData.Record.class);
    View<GenericData.Record> dest = load(datasets.get(1), GenericData.Record.class);

    CopyTask task = new CopyTask<GenericData.Record>(source, dest);
    task.setConf(getConf());

    if (noCompaction) {
      task.noCompaction();
    }

    if (numWriters >= 0) {
      task.setNumWriters(numWriters);
    }

    PipelineResult result = task.run();

    if (result.succeeded()) {
      console.info("Added {} records to \"{}\"",
          task.getCount(), datasets.get(1));
      return 0;
    } else {
      return 1;
    }
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Copy the contents of movies_avro to movies_parquet",
        "movies_avro movies_parquet",
        "# Copy the movies dataset into HBase in a map-only job",
        "movies dataset:hbase:zk-host/movies --no-compaction"
    );
  }
}
