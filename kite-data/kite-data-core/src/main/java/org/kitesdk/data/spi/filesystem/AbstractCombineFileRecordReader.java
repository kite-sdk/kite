/*
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

import java.io.IOException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.kitesdk.data.DatasetOperationException;

/**
 * Base class for wrapping file-based record readers with CombineFileInputFormat functionality. This allows multiple
 * files to be combined into a single InputSplit, with the main pre-requisite being that the InputFormat being wrapped
 * must make use of FileInputSplits.
 * <p/>
 * Sub-classes need only implement the method to instantiate an instance of their owning InputFormat class.
 */
abstract class AbstractCombineFileRecordReader<K, V> extends RecordReader<K, V> {

  private int idx;
  private RecordReader<K, V> delegate;

  public AbstractCombineFileRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer idx) {
    this.idx = idx;
  }

  /**
   * Single extension point. Returns an instance of the underlying InputFormat being used.
   *
   * @return an instance of the underlying FileInputFormat
   */
  abstract FileInputFormat<K, V> getInputFormat();

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    if (delegate != null) {
      delegate.close();
    }
    if (split instanceof CombineFileSplit) {
      CombineFileSplit combineSplit = (CombineFileSplit) split;
      FileSplit fileSplit = new FileSplit(combineSplit.getPath(idx), combineSplit.getOffset(idx),
          combineSplit.getLength(idx), combineSplit.getLocations());
      delegate = getInputFormat().createRecordReader(fileSplit, context);
      delegate.initialize(fileSplit, context);
    } else {
      throw new DatasetOperationException(
          "Split is not a CombineFileSplit: %s:%s",
          split.getClass().getCanonicalName(), split);
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return delegate.nextKeyValue();
  }

  @Override
  public K getCurrentKey() throws IOException, InterruptedException {
    return delegate.getCurrentKey();
  }

  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    return delegate.getCurrentValue();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return delegate.getProgress();
  }

  @Override
  public void close() throws IOException {
    if (delegate != null) {
      delegate.close();
      delegate = null;
    }
  }
}
