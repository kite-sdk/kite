/**
 * Copyright 2014 Cloudera Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.spi.filesystem;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.kitesdk.compat.Hadoop;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.spi.AbstractKeyRecordReaderWrapper;
import org.kitesdk.data.spi.AbstractRefinableView;
import org.kitesdk.data.spi.FilteredRecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

class FileSystemViewKeyInputFormat<E> extends InputFormat<E, Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(FileSystemViewKeyInputFormat.class);

  private FileSystemDataset<E> dataset;
  private FileSystemView<E> view;

  public FileSystemViewKeyInputFormat(FileSystemDataset<E> dataset,
                                      Configuration conf) {
    this.dataset = dataset;
    this.view = null;
    LOG.debug("Dataset: {}", dataset);

    Format format = dataset.getDescriptor().getFormat();

    AvroConfigurationUtil.configure(conf, format, dataset.getSchema(), dataset.getType());
  }

  public FileSystemViewKeyInputFormat(FileSystemView<E> view, Configuration conf) {
    this.dataset = (FileSystemDataset<E>) view.getDataset();
    this.view = view;
    LOG.debug("View: {}", view);

    Format format = dataset.getDescriptor().getFormat();

    AvroConfigurationUtil.configure(conf, format, view.getSchema(), view.getType());
  }

  @Override
  @SuppressWarnings({"unchecked", "deprecation"})
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
    Configuration conf = Hadoop.JobContext.getConfiguration.invoke(jobContext);
    Job job = new Job(conf);
    Format format = dataset.getDescriptor().getFormat();

    if (setInputPaths(jobContext, job)) {
      if (Formats.AVRO.equals(format)) {
        AvroCombineInputFormat<E> delegate = new AvroCombineInputFormat<E>();
        return delegate.getSplits(jobContext);
      } else if (Formats.PARQUET.equals(format)) {
        AvroParquetCombineInputFormat delegate = new AvroParquetCombineInputFormat();
        return delegate.getSplits(jobContext);
      } else if (Formats.JSON.equals(format)) {
        return new JSONInputFormat().getSplits(jobContext);
      } else if (Formats.CSV.equals(format)) {
        // this generates an unchecked cast exception?
        return new CSVInputFormat().getSplits(jobContext);
      } else if (Formats.INPUTFORMAT.equals(format)) {
        return InputFormatUtil.newInputFormatInstance(dataset.getDescriptor())
            .getSplits(jobContext);
      } else {
        throw new UnsupportedOperationException(
            "Not a supported format: " + format);
      }
    } else {
      return ImmutableList.of();
    }
  }

  @SuppressWarnings("unchecked")
  private boolean setInputPaths(JobContext jobContext, Job job) throws IOException {
    List<Path> paths = Lists.newArrayList((Iterator)
        (view == null ? dataset.pathIterator() : view.pathIterator()));
    LOG.debug("Input paths: {}", paths);
    if (paths.isEmpty()) {
      return false;
    }
    FileInputFormat.setInputPaths(job, paths.toArray(new Path[paths.size()]));
    // the following line is needed for Hadoop 1, otherwise the paths are not set
    Configuration contextConf = Hadoop.JobContext
        .getConfiguration.invoke(jobContext);
    Configuration jobConf = Hadoop.JobContext
        .getConfiguration.invoke(job);
    contextConf.set("mapred.input.dir", jobConf.get("mapred.input.dir"));
    return true;
  }

  @Override
  public RecordReader<E, Void> createRecordReader(InputSplit inputSplit,
                                                  TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    RecordReader<E, Void> unfilteredRecordReader = createUnfilteredRecordReader
        (inputSplit, taskAttemptContext);
    if (view != null) {
      // use the constraints to filter out entities from the reader
      return new FilteredRecordReader<E>(unfilteredRecordReader,
          ((AbstractRefinableView) view).getConstraints(), view.getAccessor());
    }
    return unfilteredRecordReader;
  }

  @SuppressWarnings("unchecked")
  private RecordReader<E, Void> createUnfilteredRecordReader(InputSplit inputSplit,
                                                             TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    Format format = dataset.getDescriptor().getFormat();
    if (Formats.AVRO.equals(format)) {
      return new AvroKeyReaderWrapper(new AvroCombineInputFormat<E>());

    } else if (Formats.PARQUET.equals(format)) {
      return new ValueReaderWrapper(new AvroParquetCombineInputFormat());

    } else if (Formats.JSON.equals(format)) {
      JSONInputFormat<E> delegate = new JSONInputFormat<E>();
      delegate.setView(view != null ? view : dataset);
      return delegate.createRecordReader(inputSplit, taskAttemptContext);

    } else if (Formats.CSV.equals(format)) {
      CSVInputFormat<E> delegate = new CSVInputFormat<E>();
      delegate.setView(view != null ? view : dataset);
      return delegate.createRecordReader(inputSplit, taskAttemptContext);

    } else if (Formats.INPUTFORMAT.equals(format)) {
      return InputFormatUtil.newRecordReader(dataset.getDescriptor());

    } else {
      throw new UnsupportedOperationException(
          "Not a supported format: " + format);
    }
  }

  private static class AvroKeyReaderWrapper<E> extends
      AbstractKeyRecordReaderWrapper<E, AvroKey<E>, NullWritable> {
    public AvroKeyReaderWrapper(AvroCombineInputFormat<E> inputFormat) {
      super(inputFormat);
    }

    @Override
    public E getCurrentKey() throws IOException, InterruptedException {
      return delegate.getCurrentKey().datum();
    }
  }

  private static class AvroCombineFileRecordReader<E> extends AbstractCombineFileRecordReader<AvroKey<E>, NullWritable> {

    public AvroCombineFileRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer idx) {
      super(split, context, idx);
    }

    @Override
    FileInputFormat<AvroKey<E>, NullWritable> getInputFormat() {
      return new AvroKeyInputFormat<E>();
    }
  }

  /**
   * Combines multiple small Avro files into a single input split.
   */
  private static class AvroCombineInputFormat<E> extends AbstractKiteCombineFileInputFormat<AvroKey<E>, NullWritable> {

    @Override
    Class<? extends AbstractCombineFileRecordReader> getRecordReaderClass() {
      return AvroCombineFileRecordReader.class;
    }
  }

  private static class AvroParquetCombineFileRecordReader<E> extends AbstractCombineFileRecordReader<Void, E> {
    public AvroParquetCombineFileRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer idx) {
      super(split, context, idx);
    }

    @Override
    FileInputFormat<Void, E> getInputFormat() {
      return new AvroParquetInputFormat<E>();
    }
  }

  /**
   * Combines multiple small Parquet files into a single input split.
   */
  private static class AvroParquetCombineInputFormat<E> extends AbstractKiteCombineFileInputFormat<Void, E> {

    @Override
    Class<? extends AbstractCombineFileRecordReader> getRecordReaderClass() {
      return AvroParquetCombineFileRecordReader.class;
    }
  }
}
