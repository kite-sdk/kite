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

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.kitesdk.compat.Hadoop;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.spi.DataModelUtil;
import org.kitesdk.data.spi.EntityAccessor;

class CSVInputFormat<E> extends FileInputFormat<E, Void> {
  private DatasetDescriptor descriptor = null;
  private EntityAccessor<E> accessor = null;

  public void setDescriptor(DatasetDescriptor descriptor) {
    this.descriptor = descriptor;
  }

  public void setType(Class<E> type) {
    this.accessor = DataModelUtil.accessor(type, descriptor.getSchema());
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    return super.getSplits(job);
  }

  @Override
  public RecordReader<E, Void> createRecordReader(InputSplit split,
                                                  TaskAttemptContext context)
      throws IOException, InterruptedException {
    Configuration conf = Hadoop.TaskAttemptContext
        .getConfiguration.invoke(context);
    Path path = ((FileSplit) split).getPath();
    CSVFileReader<E> reader = new CSVFileReader<E>(
        path.getFileSystem(conf), path, descriptor, accessor);
    reader.initialize();
    return reader.asRecordReader();
  }
}
