/*
 * Copyright 2014 Cloudera.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.spi;

import java.io.IOException;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * A wrapper class for converting a {@code RecordReader<K, V>} to a
 * {@code RecordReader<E, Void>}.
 *
 * @param <E> The {@code Dataset} entity type.
 * @param <K> The type of the key to convert.
 * @param <V> The type of the value to convert.
 * @since 0.12.0
 */
public abstract class AbstractKeyRecordReaderWrapper<E, K, V> extends RecordReader<E, Void> {

  protected RecordReader<K, V> delegate = null;
  private InputFormat<K, V> inputFormat = null;

  public AbstractKeyRecordReaderWrapper(InputFormat<K, V> inputFormat) {
    this.inputFormat = inputFormat;
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    // clean up the current wrapped reader, if present
    if (delegate != null) {
      delegate.close();
    }
    this.delegate = inputFormat.createRecordReader(inputSplit, taskAttemptContext);
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
    if (delegate != null) {
      delegate.close();
      delegate = null;
    }
  }
}
