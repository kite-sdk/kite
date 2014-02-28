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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
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
import org.kitesdk.data.View;
import org.kitesdk.data.filesystem.impl.Accessor;
import org.kitesdk.data.hbase.impl.EntityMapper;
import parquet.avro.AvroParquetInputFormat;

public class DatasetKeyInputFormat<E> extends InputFormat<E, Void>
    implements Configurable {

  public static final String KITE_REPOSITORY_URI = "kite.inputRepositoryUri";
  public static final String KITE_DATASET_NAME = "kite.inputDatasetName";

  private Configuration conf;
  private View<E> view;

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
    view = loadDataset(configuration); // TODO: load a view if specified
  }

  private static <E> Dataset<E> loadDataset(Configuration conf) {
    DatasetRepository repo = DatasetRepositories.open(conf.get(KITE_REPOSITORY_URI));
    return repo.load(conf.get(KITE_DATASET_NAME));
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR",
      justification="View field set by setConf")
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException,
      InterruptedException {
    Job job = new Job(jobContext.getConfiguration());
    if (view instanceof RandomAccessDataset) {
      TableInputFormat delegate = new TableInputFormat();
      String tableName = getTableName(view.getDataset().getName());
      jobContext.getConfiguration().set(TableInputFormat.INPUT_TABLE, tableName);
      delegate.setConf(jobContext.getConfiguration());
      // TODO: scan range
      return delegate.getSplits(jobContext);
    } else {
      Format format = view.getDataset().getDescriptor().getFormat();
      if (Formats.AVRO.equals(format)) {
        List<Path> paths = Lists.newArrayList(Accessor.getDefault().getPathIterator(view));
        FileInputFormat.setInputPaths(job, paths.toArray(new Path[paths.size()]));
        AvroJob.setInputKeySchema(job, view.getDataset().getDescriptor().getSchema());
        AvroKeyInputFormat<E> delegate = new AvroKeyInputFormat<E>();
        return delegate.getSplits(jobContext);
      } else if (Formats.PARQUET.equals(format)) {
        List<Path> paths = Lists.newArrayList(Accessor.getDefault().getPathIterator(view));
        AvroParquetInputFormat.setInputPaths(job, paths.toArray(new Path[paths.size()]));
        // TODO: use later version of parquet so we can set the schema correctly
        //AvroParquetInputFormat.setReadSchema(job, view.getDescriptor().getSchema());
        AvroParquetInputFormat delegate = new AvroParquetInputFormat();
        return delegate.getSplits(jobContext);
      } else {
        throw new UnsupportedOperationException(
            "Not a supported format: " + format);
      }
    }
  }

  // TODO: remove duplication from HBaseMetadataProvider, and see CDK-140
  static String getTableName(String name) {
    if (name.contains(".")) {
      return name.substring(0, name.indexOf('.'));
    }
    return name;
  }

  @Override
  @SuppressWarnings("unchecked")
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR",
      justification="View field set by setConf")
  public RecordReader<E, Void> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    if (view instanceof RandomAccessDataset) {
      TableInputFormat delegate = new TableInputFormat();
      delegate.setConf(taskAttemptContext.getConfiguration());
      EntityMapper<E> entityMapper =
          org.kitesdk.data.hbase.impl.Accessor.getDefault().getEntityMapper(view.getDataset());
      if (entityMapper == null) { // TODO: find entity mapper in setConf to fail early
        throw new UnsupportedOperationException(
            "Cannot find entity mapper for view: " + view);
      }
      return new HBaseRecordReaderWrapper<E>(
          delegate.createRecordReader(inputSplit, taskAttemptContext), entityMapper);
    } else {
      Format format = view.getDataset().getDescriptor().getFormat();
      if (Formats.AVRO.equals(format)) {
        AvroKeyInputFormat<E> delegate = new AvroKeyInputFormat<E>();
        return new AvroRecordReaderWrapper(
            delegate.createRecordReader(inputSplit, taskAttemptContext));
      } else if (Formats.PARQUET.equals(format)) {
        AvroParquetInputFormat delegate = new AvroParquetInputFormat();
        return new ParquetRecordReaderWrapper(
            delegate.createRecordReader(inputSplit, taskAttemptContext));
      } else {
        throw new UnsupportedOperationException(
            "Not a supported format: " + format);
      }
    }
  }

  private abstract static class AbstractRecordReader<E, K, V> extends RecordReader<E, Void> {

    protected RecordReader<K, V> delegate;

    public AbstractRecordReader(RecordReader<K, V> delegate) {
      this.delegate = delegate;
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
    public Void getCurrentValue() throws IOException, InterruptedException {
      return null;
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

  private static class AvroRecordReaderWrapper<E> extends AbstractRecordReader<E, AvroKey<E>, NullWritable> {
    public AvroRecordReaderWrapper(RecordReader<AvroKey<E>, NullWritable> delegate) {
      super(delegate);
    }

    @Override
    public E getCurrentKey() throws IOException, InterruptedException {
      return delegate.getCurrentKey().datum();
    }
  }

  private static class ParquetRecordReaderWrapper<E> extends AbstractRecordReader<E, Void, E> {
    public ParquetRecordReaderWrapper(RecordReader<Void, E> delegate) {
      super(delegate);
    }

    @Override
    public E getCurrentKey() throws IOException, InterruptedException {
      return delegate.getCurrentValue();
    }
  }

  private static class HBaseRecordReaderWrapper<E> extends AbstractRecordReader<E, ImmutableBytesWritable, Result> {
    private final EntityMapper<E> entityMapper;

    public HBaseRecordReaderWrapper(
        RecordReader<ImmutableBytesWritable, Result> delegate,
        EntityMapper<E> entityMapper) {
      super(delegate);
      this.entityMapper = entityMapper;
    }

    public E getCurrentKey() throws IOException, InterruptedException {
      return entityMapper.mapToEntity(delegate.getCurrentValue());
    }
  }
}
