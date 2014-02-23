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
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
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
import org.kitesdk.data.RandomAccessDataset;
import org.kitesdk.data.filesystem.impl.Accessor;
import org.kitesdk.data.hbase.DaoDataset;
import org.kitesdk.data.hbase.impl.BaseDao;
import org.kitesdk.data.hbase.impl.EntityMapper;
import parquet.avro.AvroParquetInputFormat;

public class DatasetKeyInputFormat<E> extends InputFormat<AvroKey<E>, NullWritable>
    implements Configurable {

  public static final String KITE_REPOSITORY_URI = "kite.inputRepositoryUri";
  public static final String KITE_DATASET_NAME = "kite.inputDatasetName";

  private Configuration conf;
  private Dataset<E> dataset;

  public static void setRepositoryUri(Job job, URI uri) {
    job.getConfiguration().set(KITE_REPOSITORY_URI, uri.toString());
  }

  public static void setDatasetName(Job job, String name) {
    job.getConfiguration().set(KITE_DATASET_NAME, name);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration configuration) {
    conf = configuration;
    dataset = loadDataset(configuration);
  }

  private static <E> Dataset<E> loadDataset(Configuration conf) {
    DatasetRepository repo = DatasetRepositories.open(conf.get(KITE_REPOSITORY_URI));
    return repo.load(conf.get(KITE_DATASET_NAME));
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException,
      InterruptedException {
    Job job = new Job(jobContext.getConfiguration());
    if (dataset instanceof RandomAccessDataset) {
      TableInputFormat delegate = new TableInputFormat();
      String tableName = dataset.getName(); // TODO: not always true
      jobContext.getConfiguration().set(TableInputFormat.INPUT_TABLE, tableName);
      delegate.setConf(jobContext.getConfiguration());
      // TODO: scan range
      return delegate.getSplits(jobContext);
    } else {
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
        // TODO: use later version of parquet so we can set the schema correctly
        //AvroParquetInputFormat.setReadSchema(job, dataset.getDescriptor().getSchema());
        AvroParquetInputFormat delegate = new AvroParquetInputFormat();
        return delegate.getSplits(jobContext);
      } else {
        throw new UnsupportedOperationException(
            "Not a supported format: " + format);
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public RecordReader<AvroKey<E>, NullWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    if (dataset instanceof RandomAccessDataset) {
      TableInputFormat delegate = new TableInputFormat();
      delegate.setConf(taskAttemptContext.getConfiguration());

      // TODO: use Accessor to get the EntityMapper and fail gracefully
      EntityMapper<E> entityMapper = ((BaseDao<E>) ((DaoDataset<E>) dataset).getDao()).getEntityMapper();
      return new EntityMapperRecordReader<E>(delegate.createRecordReader(inputSplit,
          taskAttemptContext), entityMapper);
    } else {
      Format format = dataset.getDescriptor().getFormat();
      if (Formats.AVRO.equals(format)) {
        AvroKeyInputFormat<E> delegate = new AvroKeyInputFormat<E>();
        return delegate.createRecordReader(inputSplit, taskAttemptContext);
      } else if (Formats.PARQUET.equals(format)) {
        AvroParquetInputFormat delegate = new AvroParquetInputFormat();
        return new ParquetRecordReaderWrapper(
            (RecordReader<Void, E>) delegate.createRecordReader(inputSplit, taskAttemptContext));
      } else {
        throw new UnsupportedOperationException(
            "Not a supported format: " + format);
      }
    }
  }

  private class ParquetRecordReaderWrapper extends RecordReader<AvroKey<E>, NullWritable> {

    RecordReader<Void, E> delegate;
    AvroKey<E> avroKey;

    public ParquetRecordReaderWrapper(RecordReader<Void, E> delegate) {
      this.delegate = delegate;
      this.avroKey = new AvroKey<E>();
    }

    @Override
    public void initialize(InputSplit inputSplit,
        TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
      delegate.initialize(inputSplit, taskAttemptContext);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return delegate.nextKeyValue();
    }

    @Override
    public AvroKey<E> getCurrentKey() throws IOException, InterruptedException {
      avroKey.datum(delegate.getCurrentValue());
      return avroKey;
    }

    @Override
    public NullWritable getCurrentValue() throws IOException, InterruptedException {
      return NullWritable.get();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return delegate.getProgress();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }
}
