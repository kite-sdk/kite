/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import com.beust.jcommander.internal.Maps;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import org.apache.avro.Schema;
import org.apache.crunch.DoFn;
import org.apache.crunch.PipelineResult;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.compat.DynConstructors;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.Formats;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.ColumnMappingParser;
import org.kitesdk.data.spi.PartitionStrategyParser;
import org.kitesdk.data.spi.filesystem.FileSystemDataset;
import org.kitesdk.data.spi.filesystem.InputFormatUtil;
import org.kitesdk.data.spi.filesystem.TemporaryFileSystemDatasetRepository;
import org.kitesdk.tools.CopyTask;
import org.kitesdk.tools.TaskUtil;
import org.kitesdk.tools.TransformTask;
import org.slf4j.Logger;

@Parameters(commandDescription=
    "Import records into a Dataset using an existing InputFormat")
public class InputFormatImportCommand extends BaseDatasetCommand {

  public InputFormatImportCommand(Logger console) {
    super(console);
  }

  @Parameter(description="<data path> <dataset>")
  List<String> targets;

  @Parameter(names="--format", description="An InputFormat class")
  String inFormatClass = null;

  @Parameter(names="--record-type",
      description="InputFormat argument to use as the record (key or value)")
  String recordType = InputFormatUtil.RecordType.VALUE.toString();

  @DynamicParameter(names="--set", description="Configuration settings")
  private Map<String, String> properties = Maps.newHashMap();

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
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="DM_CONVERT_CASE", justification="For record types only")
  public int run() throws IOException {
    Preconditions.checkArgument(targets != null && targets.size() == 2,
        "Data path and target dataset are required.");

    Path source = qualifiedPath(targets.get(0));
    FileSystem sourceFS = source.getFileSystem(getConf());
    Preconditions.checkArgument(sourceFS.exists(source),
        "Data path does not exist: " + source);

    // this throws IllegalArgumentException if the type is invalid.
    InputFormatUtil.RecordType.valueOf(recordType.trim().toUpperCase());

    String dataset = targets.get(1);

    View<Object> target = load(dataset, Object.class);
    Schema schema = target.getDataset().getDescriptor().getSchema();

    // Build a dataset around the incoming data
    DatasetDescriptor.Builder descriptorBuilder = new DatasetDescriptor.Builder()
        .location(source.toUri())
        .property(InputFormatUtil.INPUT_FORMAT_CLASS_PROP, inFormatClass)
        .property(InputFormatUtil.INPUT_FORMAT_RECORD_PROP, recordType)
        .format(Formats.INPUTFORMAT)
        .schema(ColumnMappingParser.removeEmbeddedMapping(
            PartitionStrategyParser.removeEmbeddedStrategy(schema)));

    for (Map.Entry<String, String> prop : properties.entrySet()) {
      descriptorBuilder.property(prop.getKey(), prop.getValue());
    }

    DatasetDescriptor inDescriptor = descriptorBuilder.build();

    TemporaryFileSystemDatasetRepository repo =
        new TemporaryFileSystemDatasetRepository(getConf(),
            // ensure the same FS as the file source is used
            sourceFS.makeQualified(new Path("/tmp")),
            target.getDataset().getNamespace(),
            UUID.randomUUID().toString());

    ClassLoader loader = loaderForJars(jars);

    try {
      FileSystemDataset<Object> inDataset = (FileSystemDataset) repo.create(
          "import", "inputformat", inDescriptor);
      Iterator<Path> iter = inDataset.pathIterator().iterator();
      Preconditions.checkArgument(iter.hasNext(),
          "Data path has no data files: " + source);

      TaskUtil.configure(getConf()).addJars(jars);

      TransformTask task;
      if (transform != null) {
        DoFn<Object, Object> transformFn;
        try {
          DynConstructors.Ctor<DoFn<Object, Object>> ctor =
              new DynConstructors.Builder(DoFn.class)
                  .loader(loader)
                  .impl(transform)
                  .buildChecked();
          transformFn = ctor.newInstance();
        } catch (NoSuchMethodException e) {
          throw new DatasetException(
              "Cannot find no-arg constructor for class: " + transform, e);
        }
        task = new TransformTask<Object, Object>(
            inDataset, target, transformFn);
      } else {
        task = new CopyTask<Object>(inDataset, target);
      }

      task.setConf(getConf());

      if (noCompaction) {
        task.noCompaction();
      }

      if (numWriters >= 0) {
        task.setNumWriters(numWriters);
      }

      PipelineResult result;
      try {
        result = runTaskWithClassLoader(task, loader);
      } catch (InterruptedException e) {
        // didn't finish
        return 1;
      }

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
      repo.delete();
    }
  }

  @Override
  public List<String> getExamples() {
    return ImmutableList.of(
        "# Import the keys of a custom input format",
        "file.sequence target_table --jar tr.jar --record-type key " +
            "--format com.company.InFormat --transform com.company.Transform"
    );
  }

  /**
   * Runs a task with the given {@link ClassLoader} as the context loader.
   *
   * @param task a {@link TransformTask}
   * @param loader a {@link ClassLoader}
   * @return the result of {@link TransformTask#run}
   * @throws IOException if the task throws an IOException
   * @throws InterruptedException if the task execution is interrupted
   */
  private static PipelineResult runTaskWithClassLoader(
      final TransformTask task, final ClassLoader loader)
      throws IOException, InterruptedException {
    RunnableFuture<PipelineResult> future = new FutureTask<PipelineResult>(
        new Callable<PipelineResult>() {
          @Override
          public PipelineResult call() throws Exception {
            return task.run();
          }
        });

    Executors.newSingleThreadExecutor(
        new ThreadFactory() {
          @Override
          public Thread newThread(Runnable r) {
            Thread taskThread = new Thread(r, "transform-task");
            taskThread.setContextClassLoader(loader);
            return taskThread;
          }
        }).execute(future);

    try {
      return future.get();
    } catch (ExecutionException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
      throw Throwables.propagate(e.getCause());
    }
  }
}
