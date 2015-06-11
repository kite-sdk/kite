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
import org.apache.crunch.PipelineResult;
import org.kitesdk.data.View;
import org.kitesdk.tools.CompactionTask;
import org.slf4j.Logger;

import static org.apache.avro.generic.GenericData.Record;

@Parameters(commandDescription="Compact all or part of a dataset")
public class CompactCommand extends BaseDatasetCommand {

  public CompactCommand(Logger console) {
    super(console);
  }

  @Parameter(description="<dataset-or-view>")
  List<String> datasets;

  @Parameter(names={"--num-writers"},
      description="The number of writer processes to use")
  int numWriters = -1;

  @Parameter(names={"--files-per-partition"},
      description="The number of files per partition to create")
  int filesPerPartition = -1;

  @Override
  public int run() throws IOException {
    Preconditions.checkArgument(datasets.size() == 1,
        "Cannot compact multiple datasets");

    String uriOrName = datasets.get(0);
    View<Record> view = load(uriOrName, Record.class);

    if (isDatasetOrViewUri(uriOrName)) {
      Preconditions.checkArgument(viewMatches(view.getUri(), uriOrName),
          "Resolved view does not match requested view: " + view.getUri());
    }

    CompactionTask task = new CompactionTask<Record>(view);

    task.setConf(getConf());

    if (numWriters >= 0) {
      task.setNumWriters(numWriters);
    }

    if (filesPerPartition > 0) {
      task.setFilesPerPartition(filesPerPartition);
    }

    PipelineResult result = task.run();

    if (result.succeeded()) {
      console.info("Compacted {} records in \"{}\"",
          task.getCount(), uriOrName);
      return 0;
    } else {
      return 1;
    }
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Compact the contents of movies",
        "movies"
    );
  }
}
