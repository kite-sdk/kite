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
package org.kitesdk.data.spi.filesystem;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.apache.avro.hadoop.io.AvroSerialization;
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
import org.kitesdk.compat.Hadoop;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.spi.AbstractKeyRecordReaderWrapper;
import org.kitesdk.data.spi.AbstractRefinableView;
import org.kitesdk.data.spi.DataModelUtil;
import org.kitesdk.data.spi.FilteredRecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.avro.AvroParquetInputFormat;

class FileSystemViewKeyInputFormat<E> extends InputFormat<E, Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(FileSystemViewKeyInputFormat.class);

  private FileSystemDataset<E> dataset;
  private FileSystemView<E> view;

  public FileSystemViewKeyInputFormat(FileSystemDataset<E> dataset,
      Configuration conf) {
    this.dataset = dataset;
    LOG.debug("Dataset: {}", dataset);

    Format format = dataset.getDescriptor().getFormat();
    if (Formats.AVRO.equals(format)) {
      AvroSerialization.setDataModelClass(conf,
          DataModelUtil.getDataModelForType(dataset.getType()).getClass());
    }
  }

  public FileSystemViewKeyInputFormat(FileSystemView<E> view, Configuration conf) {
    this((FileSystemDataset<E>) view.getDataset(), conf);
    this.view = view;
    LOG.debug("View: {}", view);
  }

  @Override
  @SuppressWarnings({"unchecked", "deprecation"})
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
    Configuration conf = Hadoop.JobContext.getConfiguration.invoke(jobContext);
    Job job = new Job(conf);
    Format format = dataset.getDescriptor().getFormat();
    if (Formats.AVRO.equals(format)) {
      setInputPaths(jobContext, job);
      AvroJob.setInputKeySchema(job, dataset.getDescriptor().getSchema());
      AvroKeyInputFormat<E> delegate = new AvroKeyInputFormat<E>();
      return delegate.getSplits(jobContext);
    } else if (Formats.PARQUET.equals(format)) {
      setInputPaths(jobContext, job);
      // TODO: use later version of parquet (with https://github.com/Parquet/parquet-mr/pull/282) so we can set the schema correctly
      // AvroParquetInputFormat.setReadSchema(job, view.getDescriptor().getSchema());
      AvroParquetInputFormat delegate = new AvroParquetInputFormat();
      return delegate.getSplits(jobContext);
    } else if (Formats.CSV.equals(format)) {
      setInputPaths(jobContext, job);
      // this generates an unchecked cast exception?
      return new CSVInputFormat().getSplits(jobContext);
    } else {
      throw new UnsupportedOperationException(
          "Not a supported format: " + format);
    }
  }

  @SuppressWarnings("unchecked")
  private void setInputPaths(JobContext jobContext, Job job) throws IOException {
    List<Path> paths =
        Lists.newArrayList(view == null ? dataset.dirIterator() : view.dirIterator());
    LOG.debug("Input paths: {}", paths);
    FileInputFormat.setInputPaths(job, paths.toArray(new Path[paths.size()]));
    // the following line is needed for Hadoop 1, otherwise the paths are not set
    Configuration contextConf = Hadoop.JobContext
        .getConfiguration.invoke(jobContext);
    Configuration jobConf = Hadoop.JobContext
        .getConfiguration.invoke(job);
    contextConf.set("mapred.input.dir", jobConf.get("mapred.input.dir"));
  }

  @Override
  public RecordReader<E, Void> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    RecordReader<E, Void> unfilteredRecordReader = createUnfilteredRecordReader
        (inputSplit, taskAttemptContext);
    if (view != null) {
      // use the constraints to filter out entities from the reader
      return new FilteredRecordReader<E>(unfilteredRecordReader,
          ((AbstractRefinableView) view).getConstraints());
    }
    return unfilteredRecordReader;
  }

  @SuppressWarnings("unchecked")
  private RecordReader<E, Void> createUnfilteredRecordReader(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    Format format = dataset.getDescriptor().getFormat();
    if (Formats.AVRO.equals(format)) {
      AvroKeyInputFormat<E> delegate = new AvroKeyInputFormat<E>();
      return new KeyReaderWrapper(
          delegate.createRecordReader(inputSplit, taskAttemptContext));
    } else if (Formats.PARQUET.equals(format)) {
      AvroParquetInputFormat delegate = new AvroParquetInputFormat();
      return new ValueReaderWrapper(
          delegate.createRecordReader(inputSplit, taskAttemptContext));
    } else if (Formats.CSV.equals(format)) {
      CSVInputFormat<E> delegate = new CSVInputFormat<E>();
      delegate.setDescriptor(dataset.getDescriptor());
      delegate.setType(dataset.getType());
      return delegate.createRecordReader(inputSplit, taskAttemptContext);
    } else {
      throw new UnsupportedOperationException(
          "Not a supported format: " + format);
    }
  }

  private static class KeyReaderWrapper<E> extends
      AbstractKeyRecordReaderWrapper<E, AvroKey<E>, NullWritable> {
    public KeyReaderWrapper(RecordReader<AvroKey<E>, NullWritable> delegate) {
      super(delegate);
    }

    @Override
    public E getCurrentKey() throws IOException, InterruptedException {
      return delegate.getCurrentKey().datum();
    }
  }

  private static class ValueReaderWrapper<E> extends
      AbstractKeyRecordReaderWrapper<E, Void, E> {
    public ValueReaderWrapper(RecordReader<Void, E> delegate) {
      super(delegate);
    }

    @Override
    public E getCurrentKey() throws IOException, InterruptedException {
      return delegate.getCurrentValue();
    }
  }

}
