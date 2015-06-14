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

package org.kitesdk.tools;

import com.google.common.base.Preconditions;
import java.io.IOException;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Target;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.Replaceable;

/**
 * @since 1.1.0
 */
public class CompactionTask<T> implements Configurable {

  private final CopyTask<T> task;

  public CompactionTask(View<T> view) {
    checkCompactable(view);
    this.task = new CopyTask<T>(view, view);
    task.setWriteMode(Target.WriteMode.OVERWRITE);
  }

  public long getCount() {
    return task.getCount();
  }

  public CompactionTask setNumWriters(int numWriters) {
    task.setNumWriters(numWriters);
    return this;
  }

  public PipelineResult run() throws IOException {
    return task.run();
  }

  @Override
  public void setConf(Configuration configuration) {
    task.setConf(configuration);
  }

  @Override
  public Configuration getConf() {
    return task.getConf();
  }

  public void setFilesPerPartition(int filesPerPartition) {
    task.setFilesPerPartition(filesPerPartition);
  }

  @SuppressWarnings("unchecked")
  private void checkCompactable(View<T> view) {
    Dataset<T> dataset = view.getDataset();
    if (!(dataset instanceof Replaceable)) {
      throw new IllegalArgumentException("Cannot compact dataset: " + dataset);
    }
    Replaceable<View<T>> replaceable = ((Replaceable<View<T>>) dataset);
    Preconditions.checkArgument(replaceable.canReplace(view),
        "Cannot compact view: " + view);
  }
}
