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

import com.google.common.collect.Lists;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.kitesdk.data.DatasetOperationException;

/**
 * A Kite-specific subclass of CombineFileInputFormat and CombineFileRecordReader to work around the fact that
 * Crunch does special handling of CombineFileSplits internally.
 * <p/>
 * The classes in this file don't add any additional functionality to CombineFileInputFormat or CombineFileRecordReader,
 * they just ensure that KiteCombineFileSplits are being used (instead of CombineFileSplit) to avoid CrunchInputFormat
 * from interpreting them as coming from Crunch.
 */
abstract class AbstractKiteCombineFileInputFormat<K, V> extends CombineFileInputFormat<K, V> {

  /**
   * A wrapper around CombineFileSplit which simply allows us to avoid using CombineFileSplit directly, as Crunch
   * has special handling for CombineFileSplit.
   */
  static class KiteCombineFileSplit extends InputSplit implements Writable {

    private CombineFileSplit delegate;

    public KiteCombineFileSplit() {
      // Needed for Writable interface
    }

    public KiteCombineFileSplit(CombineFileSplit delegate) {
      this.delegate = delegate;
    }


    public void readFields(DataInput in) throws IOException {
      delegate = new CombineFileSplit();
      delegate.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
      delegate.write(out);
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
      return delegate.getLength();
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return delegate.getLocations();
    }

    CombineFileSplit getCombineFileSplit() {
      return delegate;
    }
  }

  abstract Class<? extends AbstractCombineFileRecordReader> getRecordReaderClass();

  private static class KiteCombineFileRecordReader extends CombineFileRecordReader {

    @SuppressWarnings("unchecked")
    public KiteCombineFileRecordReader(KiteCombineFileSplit kiteCombineSplit, TaskAttemptContext context, Class rrClass) throws IOException {
      super(kiteCombineSplit.getCombineFileSplit(), context, rrClass);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      if (split instanceof KiteCombineFileSplit) {
        KiteCombineFileSplit kiteCombineFileSplit = (KiteCombineFileSplit) split;
        super.initialize(kiteCombineFileSplit.getCombineFileSplit(), context);
      } else {
        throw new DatasetOperationException(
            "Split is not a KiteCombineFileSplit: %s:%s",
            split.getClass().getCanonicalName(), split);
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext taskContext) throws IOException {
    if (split instanceof KiteCombineFileSplit) {
      return new KiteCombineFileRecordReader(
          (KiteCombineFileSplit) split, taskContext, getRecordReaderClass());
    } else {
      throw new DatasetOperationException(
          "Split is not a KiteCombineFileSplit: %s:%s",
          split.getClass().getCanonicalName(), split);
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<InputSplit> kiteCombineSplits = Lists.newArrayList();
    for (InputSplit inputSplit : super.getSplits(job)) {
      kiteCombineSplits.add(new KiteCombineFileSplit((CombineFileSplit) inputSplit));
    }
    return kiteCombineSplits;
  }
}
