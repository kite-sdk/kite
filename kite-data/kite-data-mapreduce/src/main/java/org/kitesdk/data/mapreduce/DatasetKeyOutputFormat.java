/**
 * Copyright 2014 Cloudera Inc.
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
package org.kitesdk.data.mapreduce;

import com.google.common.annotations.Beta;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.kitesdk.compat.Hadoop;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.PartitionKey;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.AbstractDataset;
import org.kitesdk.data.spi.AbstractRefinableView;
import org.kitesdk.data.spi.Constraints;
import org.kitesdk.data.spi.Mergeable;
import org.kitesdk.data.spi.URIBuilder;
import org.kitesdk.data.spi.filesystem.FileSystemDataset;

/**
 * A MapReduce {@code OutputFormat} for writing to a {@link Dataset}.
 *
 * Since a {@code Dataset} only contains entities (not key/value pairs), this output
 * format ignores the value.
 *
 * @param <E> The type of entities in the {@code Dataset}.
 */
@Beta
public class DatasetKeyOutputFormat<E> extends OutputFormat<E, Void> {

  public static final String KITE_OUTPUT_URI = "kite.outputUri";
  @Deprecated
  public static final String KITE_REPOSITORY_URI = "kite.outputRepositoryUri";
  @Deprecated
  public static final String KITE_DATASET_NAME = "kite.outputDatasetName";
  public static final String KITE_PARTITION_DIR = "kite.outputPartitionDir";
  public static final String KITE_CONSTRAINTS = "kite.outputConstraints";

  public static class ConfigBuilder {
    private final Configuration conf;

    private ConfigBuilder(Job job) {
      this.conf = Hadoop.JobContext.getConfiguration.invoke(job);
    }

    private ConfigBuilder(Configuration conf) {
      this.conf = conf;
    }

    public ConfigBuilder writeTo(URI viewUri) {
      conf.set(KITE_OUTPUT_URI, viewUri.toString());
      return this;
    }

    public ConfigBuilder writeTo(View<?> view) {
      if (view instanceof Dataset) {
        if (view instanceof FileSystemDataset) {
          FileSystemDataset dataset = (FileSystemDataset) view;
          conf.set(KITE_PARTITION_DIR,
              String.valueOf(dataset.getDescriptor().getLocation()));
        }
      } else if (view instanceof AbstractRefinableView) {
        conf.set(KITE_CONSTRAINTS,
            Constraints.serialize(((AbstractRefinableView) view).getConstraints()));
      } else {
        throw new UnsupportedOperationException("Implementation " +
            "does not provide OutputFormat support. View: " + view);
      }
      return writeTo(view.getDataset().getUri());
    }

    public ConfigBuilder writeTo(String viewUri) {
      return writeTo(URI.create(viewUri));
    }
  }

  /**
   * Sets the output dataset that will be used for the given Job.
   * @param job
   */
  public static ConfigBuilder configure(Job job) {
    job.setOutputFormatClass(DatasetKeyOutputFormat.class);
    return new ConfigBuilder(job);
  }

  /**
   * Sets the output dataset that will be used for the given Configuration.
   * @param conf
   */
  public static ConfigBuilder configure(Configuration conf) {
    return new ConfigBuilder(conf);
  }

  /**
   * @deprecated will be removed in 0.16.0; use {@link #configure(Job)} instead
   */
  public static void setRepositoryUri(Job job, URI uri) {
    Configuration conf = Hadoop.JobContext.getConfiguration.invoke(job);
    conf.set(KITE_REPOSITORY_URI, uri.toString());
    conf.unset(KITE_OUTPUT_URI); // this URI would override, so remove it
  }

  /**
   * @deprecated will be removed in 0.16.0; use {@link #configure(Job)} instead
   */
  public static void setDatasetName(Job job, String name) {
    Configuration conf = Hadoop.JobContext.getConfiguration.invoke(job);
    conf.set(KITE_DATASET_NAME, name);
    conf.unset(KITE_OUTPUT_URI); // this URI would override, so remove it
  }

  /**
   * @deprecated will be removed in 0.16.0; use {@link #configure(Job)} instead
   */
  public static <E> void setView(Job job, View<E> view) {
    configure(job).writeTo(view);
  }

  /**
   * @deprecated will be removed in 0.16.0; use {@link #configure(Configuration)}
   */
  public static <E> void setView(Configuration conf, View<E> view) {
    configure(conf).writeTo(view);
  }

  static class DatasetRecordWriter<E> extends RecordWriter<E, Void> {

    private DatasetWriter<E> datasetWriter;

    public DatasetRecordWriter(View<E> view) {
      this.datasetWriter = view.newWriter();
      this.datasetWriter.open();
    }

    @Override
    public void write(E key, Void v) {
      datasetWriter.write(key);
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) {
      datasetWriter.close();
    }
  }

  static class NullOutputCommitter extends OutputCommitter {
    @Override
    public void setupJob(JobContext jobContext) { }

    @Override
    public void setupTask(TaskAttemptContext taskContext) { }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext) {
      return false;
    }

    @Override
    public void commitTask(TaskAttemptContext taskContext) { }

    @Override
    public void abortTask(TaskAttemptContext taskContext) { }
  }

  static class MergeOutputCommitter<E> extends OutputCommitter {
    @Override
    public void setupJob(JobContext jobContext) {
      createJobDataset(jobContext);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void commitJob(JobContext jobContext) throws IOException {
      View<E> targetView = load(jobContext);
      Dataset<E> jobDataset = loadJobDataset(jobContext);
      ((Mergeable<Dataset<E>>) targetView.getDataset()).merge(jobDataset);
      deleteJobDataset(jobContext);
    }

    @Override
    public void abortJob(JobContext jobContext, JobStatus.State state) {
      deleteJobDataset(jobContext);
    }

    @Override
    public void setupTask(TaskAttemptContext taskContext) {
      // do nothing: the task attempt dataset is created in getRecordWriter
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext) {
      return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void commitTask(TaskAttemptContext taskContext) throws IOException {
      Dataset<E> taskAttemptDataset = loadTaskAttemptDataset(taskContext);
      if (taskAttemptDataset != null) {
        Dataset<E> jobDataset = loadJobDataset(taskContext);
        ((Mergeable<Dataset<E>>) jobDataset).merge(taskAttemptDataset);
        deleteTaskAttemptDataset(taskContext);
      }
    }

    @Override
    public void abortTask(TaskAttemptContext taskContext) {
      deleteTaskAttemptDataset(taskContext);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public RecordWriter<E, Void> getRecordWriter(TaskAttemptContext taskAttemptContext) {
    Configuration conf = Hadoop.TaskAttemptContext
        .getConfiguration.invoke(taskAttemptContext);
    View<E> target = load(taskAttemptContext);
    View<E> working;

    if (usePerTaskAttemptDatasets(target)) {
      working = loadOrCreateTaskAttemptDataset(taskAttemptContext);
    } else {
      working = target;
    }

    String partitionDir = conf.get(KITE_PARTITION_DIR);
    String constraintsString = conf.get(KITE_CONSTRAINTS);
    if (working.getDataset().getDescriptor().isPartitioned() &&
        partitionDir != null) {
      PartitionKey key = ((FileSystemDataset) target).keyFromDirectory(
          new Path(partitionDir));
      if (key != null) {
        working = working.getDataset().getPartition(key, true);
      }
      return new DatasetRecordWriter<E>(working);
    } else if (constraintsString != null) {
      Constraints constraints = Constraints.deserialize(constraintsString);
      if (working instanceof AbstractDataset) {
        return new DatasetRecordWriter<E>(((AbstractDataset) working).filter(constraints));
      }
      throw new DatasetException("Cannot find view from constraints for " + working);
    } else {
      return new DatasetRecordWriter<E>(working);
    }
  }

  @Override
  public void checkOutputSpecs(JobContext jobContext) {
    // always run
    // The committer setup will fail if the output dataset does not exist
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) {
    View<E> view = load(taskAttemptContext);
    return usePerTaskAttemptDatasets(view) ?
        new MergeOutputCommitter<E>() : new NullOutputCommitter();
  }

  private static <E> boolean usePerTaskAttemptDatasets(View<E> target) {
    // new API output committers are not called properly in Hadoop 1
    return !isHadoop1() && target.getDataset() instanceof Mergeable;
  }

  private static boolean isHadoop1() {
    return !JobContext.class.isInterface();
  }

  private static DatasetRepository getDatasetRepository(JobContext jobContext) {
    Configuration conf = Hadoop.JobContext.getConfiguration.invoke(jobContext);
    return Datasets.repositoryFor(conf.get(KITE_OUTPUT_URI));
  }

  private static String getJobDatasetName(JobContext jobContext) {
    return jobContext.getJobID().toString();
  }

  private static String getTaskAttemptDatasetName(TaskAttemptContext taskContext) {
    return taskContext.getTaskAttemptID().toString();
  }

  @SuppressWarnings("deprecation")
  private static <E> View<E> load(JobContext jobContext) {
    Configuration conf = Hadoop.JobContext.getConfiguration.invoke(jobContext);
    String outputUri = conf.get(KITE_OUTPUT_URI);
    if (outputUri == null) {
      return Datasets.<E, View<E>>view(
          new URIBuilder(
              conf.get(KITE_REPOSITORY_URI), conf.get(KITE_DATASET_NAME))
              .build());
    }
    return Datasets.<E, View<E>>view(outputUri);
  }

  private static <E> Dataset<E> createJobDataset(JobContext jobContext) {
    Dataset<Object> dataset = load(jobContext).getDataset();
    String jobDatasetName = getJobDatasetName(jobContext);
    DatasetRepository repo = getDatasetRepository(jobContext);
    return repo.create(jobDatasetName, copy(dataset.getDescriptor()));
  }

  private static <E> Dataset<E> loadJobDataset(JobContext jobContext) {
    DatasetRepository repo = getDatasetRepository(jobContext);
    return repo.load(getJobDatasetName(jobContext));
  }

  private static void deleteJobDataset(JobContext jobContext) {
    DatasetRepository repo = getDatasetRepository(jobContext);
    repo.delete(getJobDatasetName(jobContext));
  }

  private static <E> Dataset<E> loadOrCreateTaskAttemptDataset(TaskAttemptContext taskContext) {
    String taskAttemptDatasetName = getTaskAttemptDatasetName(taskContext);
    DatasetRepository repo = getDatasetRepository(taskContext);
    Dataset<E> jobDataset = loadJobDataset(taskContext);
    if (repo.exists(taskAttemptDatasetName)) {
      return repo.load(taskAttemptDatasetName);
    } else {
      return repo.create(taskAttemptDatasetName, copy(jobDataset.getDescriptor()));
    }
  }

  private static <E> Dataset<E> loadTaskAttemptDataset(TaskAttemptContext taskContext) {
    DatasetRepository repo = getDatasetRepository(taskContext);
    String taskAttemptDatasetName = getTaskAttemptDatasetName(taskContext);
    if (repo.exists(taskAttemptDatasetName)) {
      return repo.load(taskAttemptDatasetName);
    }
    return null;
  }

  private static void deleteTaskAttemptDataset(TaskAttemptContext taskContext) {
    DatasetRepository repo = getDatasetRepository(taskContext);
    String taskAttemptDatasetName = getTaskAttemptDatasetName(taskContext);
    if (repo.exists(taskAttemptDatasetName)) {
      repo.delete(taskAttemptDatasetName);
    }
  }

  private static DatasetDescriptor copy(DatasetDescriptor descriptor) {
    // location must be null when creating a new dataset
    return new DatasetDescriptor.Builder(descriptor).location((URI) null).build();
  }

}
