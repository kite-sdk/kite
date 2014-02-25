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
package org.kitesdk.data.crunch;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetRepositories;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.PartitionKey;
import org.kitesdk.data.RandomAccessDataset;
import org.kitesdk.data.filesystem.impl.Accessor;

import java.io.IOException;
import java.net.URI;

public class DatasetOutputFormat<E> extends OutputFormat<AvroKey<E>, NullWritable> {

  public static final String KITE_REPOSITORY_URI = "kite.repositoryUri";
  public static final String KITE_DATASET_NAME = "kite.datasetName";
  public static final String KITE_PARTITION_DIR = "kite.partitionDir";

  static class DatasetRecordWriter<E> extends RecordWriter<AvroKey<E>, NullWritable> {

    private DatasetWriter<E> datasetWriter;

    public DatasetRecordWriter(Dataset<E> dataset) {
      this.datasetWriter = dataset.newWriter();
      this.datasetWriter.open();
    }

    @Override
    public void write(AvroKey<E> key, NullWritable v) {
      datasetWriter.write(key.datum());
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

  static class DatasetOutputCommitter<E> extends OutputCommitter {
    @Override
    public void setupJob(JobContext jobContext) {
      createJobDataset(jobContext);
    }

    @Override
    public void commitJob(JobContext jobContext) throws IOException {
      Dataset<E> dataset = loadDataset(jobContext);
      Dataset<E> jobDataset = loadJobDataset(jobContext);
      Accessor.getDefault().merge(dataset, jobDataset);
      deleteJobDataset(jobContext);
    }

    @Override
    public void abortJob(JobContext jobContext, JobStatus.State state) {
      deleteJobDataset(jobContext);
    }

    @Override
    public void setupTask(TaskAttemptContext taskContext) {
      createTaskAttemptDataset(taskContext);
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext) {
      return true;
    }

    @Override
    public void commitTask(TaskAttemptContext taskContext) throws IOException {
      Dataset<E> jobDataset = loadJobDataset(taskContext);
      Dataset<E> taskAttemptDataset = loadTaskAttemptDataset(taskContext);
      Accessor.getDefault().merge(jobDataset, taskAttemptDataset);
      deleteTaskAttemptDataset(taskContext);
    }

    @Override
    public void abortTask(TaskAttemptContext taskContext) {
      deleteTaskAttemptDataset(taskContext);
    }
  }

  @Override
  public RecordWriter<AvroKey<E>, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) {
    Configuration conf = taskAttemptContext.getConfiguration();
    Dataset<E> dataset = loadDataset(taskAttemptContext);

    if (!(dataset instanceof RandomAccessDataset)) {
      // use per-task attempt datasets for filesystem datasets
      dataset = loadTaskAttemptDataset(taskAttemptContext);
    }

    // TODO: the following should generalize with views
    String partitionDir = conf.get(KITE_PARTITION_DIR);
    if (dataset.getDescriptor().isPartitioned() && partitionDir != null) {
      PartitionKey key = Accessor.getDefault().fromDirectoryName(dataset, new Path(partitionDir));
      if (key != null) {
        dataset = dataset.getPartition(key, true);
      }
    }

    return new DatasetRecordWriter<E>(dataset);
  }

  @Override
  public void checkOutputSpecs(JobContext jobContext) {
    // always run
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) {
    Dataset<E> dataset = loadDataset(taskAttemptContext);
    return (dataset instanceof RandomAccessDataset) ?
        new NullOutputCommitter() : new DatasetOutputCommitter<E>();
  }

  private static DatasetRepository getDatasetRepository(JobContext jobContext) {
    Configuration conf = jobContext.getConfiguration();
    return DatasetRepositories.open(conf.get(KITE_REPOSITORY_URI));
  }

  private static String getJobDatasetName(JobContext jobContext) {
    Configuration conf = jobContext.getConfiguration();
    return conf.get(KITE_DATASET_NAME) + "-" + jobContext.getJobID().toString();
  }

  private static String getTaskAttemptDatasetName(TaskAttemptContext taskContext) {
    Configuration conf = taskContext.getConfiguration();
    return conf.get(KITE_DATASET_NAME) + "-" + taskContext.getTaskAttemptID().toString();
  }

  private static <E> Dataset<E> loadDataset(JobContext jobContext) {
    Configuration conf = jobContext.getConfiguration();
    DatasetRepository repo = getDatasetRepository(jobContext);
    return repo.load(conf.get(KITE_DATASET_NAME));
  }

  private static <E> Dataset<E> createJobDataset(JobContext jobContext) {
    Dataset<Object> dataset = loadDataset(jobContext);
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

  private static <E> Dataset<E> createTaskAttemptDataset(TaskAttemptContext taskContext) {
    Dataset<Object> dataset = loadDataset(taskContext);
    String taskAttemptDatasetName = getTaskAttemptDatasetName(taskContext);
    DatasetRepository repo = getDatasetRepository(taskContext);
    return repo.create(taskAttemptDatasetName, copy(dataset.getDescriptor()));
  }

  private static <E> Dataset<E> loadTaskAttemptDataset(TaskAttemptContext taskContext) {
    DatasetRepository repo = getDatasetRepository(taskContext);
    return repo.load(getTaskAttemptDatasetName(taskContext));
  }

  private static void deleteTaskAttemptDataset(TaskAttemptContext taskContext) {
    DatasetRepository repo = getDatasetRepository(taskContext);
    repo.delete(getTaskAttemptDatasetName(taskContext));
  }

  private static DatasetDescriptor copy(DatasetDescriptor descriptor) {
    // location must be null when creating a new dataset
    return new DatasetDescriptor.Builder(descriptor).location((URI) null).build();
  }

}
