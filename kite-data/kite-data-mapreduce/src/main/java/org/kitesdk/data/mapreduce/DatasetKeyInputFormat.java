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

import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetRepositories;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.filesystem.impl.Accessor;
import parquet.avro.AvroParquetInputFormat;

public class DatasetKeyInputFormat<E> extends InputFormat<AvroKey<E>, NullWritable> {

  public static final String KITE_REPOSITORY_URI = "kite.inputRepositoryUri";
  public static final String KITE_DATASET_NAME = "kite.inputDatasetName";

  public static void setRepositoryUri(Job job, URI uri) {
    job.getConfiguration().set(KITE_REPOSITORY_URI, uri.toString());
  }

  public static void setDatasetName(Job job, String name) {
    job.getConfiguration().set(KITE_DATASET_NAME, name);
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException,
      InterruptedException {
    Job job = new Job(jobContext.getConfiguration());
    Dataset<E> dataset = loadDataset(jobContext);
    Format format = dataset.getDescriptor().getFormat();
    if (Formats.AVRO.equals(format)) {
      List<Path> paths = Lists.newArrayList(Accessor.getDefault().getPathIterator(dataset));
      FileInputFormat.setInputPaths(job, paths.toArray(new Path[paths.size()]));
      AvroJob.setInputKeySchema(job, dataset.getDescriptor().getSchema());
      AvroKeyInputFormat<E> delegate = new AvroKeyInputFormat<E>();
      return delegate.getSplits(jobContext);
    } else if (Formats.PARQUET.equals(format)) {
      List<Path> paths = Lists.newArrayList(Accessor.getDefault().getPathIterator(dataset));
      AvroParquetInputFormat.setInputPaths(job, paths.toArray(new Path[paths.size()]));

      // TODO: use later version of parquet
      //AvroParquetInputFormat.setReadSchema(job, dataset.getDescriptor().getSchema());
      AvroParquetInputFormat delegate = new AvroParquetInputFormat();
      return delegate.getSplits(jobContext);
    } else {
      throw new UnsupportedOperationException(
          "Not a supported format: " + format);
    }
  }

  @Override
  public RecordReader<AvroKey<E>, NullWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    Dataset<E> dataset = loadDataset(taskAttemptContext);
    Format format = dataset.getDescriptor().getFormat();
    if (Formats.AVRO.equals(format)) {
      AvroKeyInputFormat<E> delegate = new AvroKeyInputFormat<E>();
      return delegate.createRecordReader(inputSplit, taskAttemptContext);
//    } else if (Formats.PARQUET.equals(format)) {
//      AvroParquetInputFormat delegate = new AvroParquetInputFormat();
//      // TODO: wrap record reader to make types come out right
//      return delegate.createRecordReader(inputSplit, taskAttemptContext);
    } else {
      throw new UnsupportedOperationException(
          "Not a supported format: " + format);
    }
  }

  private static DatasetRepository getDatasetRepository(JobContext jobContext) {
    Configuration conf = jobContext.getConfiguration();
    return DatasetRepositories.open(conf.get(KITE_REPOSITORY_URI));
  }

  private static <E> Dataset<E> loadDataset(JobContext jobContext) {
    Configuration conf = jobContext.getConfiguration();
    DatasetRepository repo = getDatasetRepository(jobContext);
    return repo.load(conf.get(KITE_DATASET_NAME));
  }
}
