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

package org.kitesdk.data.spi.filesystem;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.kitesdk.compat.Hadoop;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.DatasetOperationException;
import org.kitesdk.data.spi.AbstractDatasetReader;
import org.kitesdk.data.spi.ReaderWriterState;

public class InputFormatReader<E> extends AbstractDatasetReader<E> {
  private static final TaskAttemptID FAKE_ID =
      new TaskAttemptID("", 0, false, 0, 0);

  private final FileSystem fs;
  private final Path path;
  private final Configuration conf;
  private final DatasetDescriptor descriptor;
  private final TaskAttemptContext attemptContext;

  // reader state
  private ReaderWriterState state = ReaderWriterState.NEW;
  private Iterator<InputSplit> splits;
  private RecordReader<E, Void> currentReader = null;
  private boolean hasNext = false;
  private boolean shouldAdvance = false;

  public InputFormatReader(FileSystem fs, Path path, DatasetDescriptor descriptor) {
    this.fs = fs;
    this.path = path;
    this.descriptor = descriptor;
    this.state = ReaderWriterState.NEW;

    // set up the configuration from the descriptor properties
    this.conf = new Configuration(fs.getConf());
    for (String prop : descriptor.listProperties()) {
      conf.set(prop, descriptor.getProperty(prop));
    }

    this.attemptContext = Hadoop.TaskAttemptContext.ctor.newInstance(conf, FAKE_ID);
  }

  @Override
  public void initialize() {
    Preconditions.checkState(ReaderWriterState.NEW.equals(state),
        "A reader may not be opened more than once - current state:%s", state);

    try {
      FileInputFormat format = InputFormatUtil.newInputFormatInstance(descriptor);
      Job job = Hadoop.Job.newInstance.invoke(conf);

      FileInputFormat.addInputPath(job, path);
      // attempt to minimize the number of InputSplits
      FileStatus stat = fs.getFileStatus(path);
      FileInputFormat.setMaxInputSplitSize(job, stat.getLen());

      this.splits = format.getSplits(job).iterator();
      this.shouldAdvance = true;
      this.state = ReaderWriterState.OPEN;

    } catch (RuntimeException e) {
      this.state = ReaderWriterState.ERROR;
      throw new DatasetOperationException("Cannot calculate splits", e);
    } catch (IOException e) {
      this.state = ReaderWriterState.ERROR;
      throw new DatasetIOException("Cannot calculate splits", e);
    }
  }

  @Override
  public boolean hasNext() {
    Preconditions.checkState(ReaderWriterState.OPEN.equals(state),
        "Attempt to read from a file in state:%s", state);

    // the Iterator contract requires that calls to hasNext() not change the
    // iterator state. calling next() should advance the iterator. however,
    // this wraps a RecordReader that reuses objects, so advancing in next
    // after retrieving the key/value pair mutates the pair. this hack is a way
    // to advance once per call to next(), but do it as late as possible.
    if (shouldAdvance) {
      this.hasNext = advance();
      this.shouldAdvance = false;
    }
    return hasNext;
  }

  @Override
  public E next() {
    Preconditions.checkState(ReaderWriterState.OPEN.equals(state),
        "Attempt to read from a file in state:%s", state);

    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    try {
      E record = currentReader.getCurrentKey();

      this.shouldAdvance = true;

      return record;
    } catch (RuntimeException e) {
      this.state = ReaderWriterState.ERROR;
      throw new DatasetOperationException("Cannot get record", e);
    } catch (IOException e) {
      this.state = ReaderWriterState.ERROR;
      throw new DatasetIOException("Cannot get record", e);
    } catch (InterruptedException e) {
      // don't swallow the interrupt
      Thread.currentThread().interrupt();
      // error: it is unclear whether the underlying reader is valid
      this.state = ReaderWriterState.ERROR;
      throw new DatasetOperationException("Interrupted", e);
    }
  }

  private boolean advance() {
    try {
      if (currentReader != null && currentReader.nextKeyValue()) {
        return true;
      } else {
        if (currentReader == null) {
          this.currentReader = InputFormatUtil.newRecordReader(descriptor);
        }
        while (splits.hasNext()) {
          // advance the reader and see if it has records
          InputSplit nextSplit = splits.next();
          currentReader.initialize(nextSplit, attemptContext);
          if (currentReader.nextKeyValue()) {
            return true;
          }
        }
        // either no next split or all readers were empty
        return false;
      }
    } catch (RuntimeException e) {
      this.state = ReaderWriterState.ERROR;
      throw new DatasetOperationException("Cannot advance reader", e);
    } catch (IOException e) {
      this.state = ReaderWriterState.ERROR;
      throw new DatasetIOException("Cannot advance reader", e);
    } catch (InterruptedException e) {
      // don't swallow the interrupt
      Thread.currentThread().interrupt();
      // error: it is unclear whether the underlying reader is valid
      this.state = ReaderWriterState.ERROR;
      throw new DatasetOperationException("Interrupted", e);
    }
  }

  @Override
  public void close() {
    if (!state.equals(ReaderWriterState.OPEN)) {
      return;
    }

    this.state = ReaderWriterState.CLOSED;

    try {
      if (currentReader != null) {
        currentReader.close();
      }
    } catch (IOException e) {
      throw new DatasetIOException("Unable to close reader path:" + path, e);
    }

    this.hasNext = false;
  }

  @Override
  public boolean isOpen() {
    return (ReaderWriterState.OPEN == state);
  }
}
