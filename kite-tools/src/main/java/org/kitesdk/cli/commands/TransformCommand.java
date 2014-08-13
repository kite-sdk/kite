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
import org.apache.crunch.DoFn;
import org.apache.crunch.PipelineResult;
import org.kitesdk.compat.DynConstructors;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.View;
import org.kitesdk.tools.CopyTask;
import org.kitesdk.tools.TaskUtil;
import org.kitesdk.tools.TransformTask;
import org.slf4j.Logger;

import static org.apache.avro.generic.GenericData.Record;

@Parameters(commandDescription=
    "Transform records from one Dataset and store them in another")
public class TransformCommand extends BaseDatasetCommand {

  public TransformCommand(Logger console) {
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

  @Parameter(names={"--transform"},
      description="A transform DoFn class name")
  String transform = null;

  @Parameter(names="--jar",
      description="Add a jar to the runtime classpath")
  List<String> jars;

  @Override
  public int run() throws IOException {
    Preconditions.checkArgument(datasets != null && datasets.size() > 1,
        "Source and target datasets are required");
    Preconditions.checkArgument(datasets.size() == 2,
        "Cannot copy multiple datasets");

    View<Record> source = load(datasets.get(0), Record.class);
    View<Record> dest = load(datasets.get(1), Record.class);

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
      task = new TransformTask<Record, Record>(source, dest, transformFn);
    } else {
      task = new CopyTask<Record>(source, dest);
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
        "# Transform the contents of movies_src using com.example.TransformFn",
        "movies_src movies --transform com.example.TransformFn --jar fns.jar"
    );
  }
}
