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
package org.kitesdk.data.spi;

import com.google.common.base.Predicate;
import java.io.IOException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FilteredRecordReader<E> extends RecordReader<E, Void> {
  private RecordReader<E, Void> unfiltered;
  private Predicate<E> predicate;
  private E current;

  public FilteredRecordReader(RecordReader<E, Void> unfiltered,
                              Constraints constraints,
                              EntityAccessor<E> accessor) {
    this.unfiltered = unfiltered;
    this.predicate = constraints.toEntityPredicate(accessor); // TODO: optimize with storage key
  }

  @Override
  public void initialize(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    unfiltered.initialize(inputSplit, taskAttemptContext);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    current = computeNextKey();
    return current != null;
  }

  private E computeNextKey() throws IOException, InterruptedException {
    while (unfiltered.nextKeyValue()) {
      E element = unfiltered.getCurrentKey();
      if (predicate.apply(element)) {
        return element;
      }
    }
    return null;
  }

  @Override
  public E getCurrentKey() throws IOException, InterruptedException {
    return current;
  }

  @Override
  public Void getCurrentValue() throws IOException, InterruptedException {
    return unfiltered.getCurrentValue();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return unfiltered.getProgress();
  }

  @Override
  public void close() throws IOException {
    unfiltered.close();
  }
}
