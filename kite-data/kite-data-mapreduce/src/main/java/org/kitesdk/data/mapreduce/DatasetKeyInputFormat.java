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

import com.google.common.annotations.Beta;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetRepositories;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.PartitionKey;
import org.kitesdk.data.filesystem.impl.Accessor;
import org.kitesdk.data.spi.AbstractDataset;

/**
 * A MapReduce {@code InputFormat} for reading from a {@link Dataset}.
 *
 * Since a {@code Dataset} only contains entities (not key/value pairs), this output
 * format ignores the value.
 *
 * @param <E> The type of entities in the {@code Dataset}.
 */
@Beta
public class DatasetKeyInputFormat<E> extends InputFormat<E, Void>
    implements Configurable {

  public static final String KITE_REPOSITORY_URI = "kite.inputRepositoryUri";
  public static final String KITE_DATASET_NAME = "kite.inputDatasetName";
  public static final String KITE_PARTITION_DIR = "kite.inputPartitionDir";

  private Configuration conf;
  private InputFormat<E, Void> delegate;

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
    Dataset<E> dataset = loadDataset(configuration);

    // TODO: the following should generalize with views
    String partitionDir = conf.get(KITE_PARTITION_DIR);
    if (dataset.getDescriptor().isPartitioned() && partitionDir != null) {
      PartitionKey key = Accessor.getDefault().fromDirectoryName(dataset, new Path(partitionDir));
      if (key != null) {
        dataset = dataset.getPartition(key, true);
      }
    }

    if (dataset instanceof AbstractDataset) {
      delegate = ((AbstractDataset<E>) dataset).getDelegateInputFormat();
    } else {
      throw new UnsupportedOperationException("Incompatible Dataset: implementation " +
          "does not provide InputFormat support. Dataset: " + dataset);
    }
  }

  private static <E> Dataset<E> loadDataset(Configuration conf) {
    DatasetRepository repo = DatasetRepositories.open(conf.get(KITE_REPOSITORY_URI));
    return repo.load(conf.get(KITE_DATASET_NAME));
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR",
      justification="Delegate set by setConf")
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException,
      InterruptedException {
    return delegate.getSplits(jobContext);
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR",
      justification="Delegate set by setConf")
  public RecordReader<E, Void> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    return delegate.createRecordReader(inputSplit, taskAttemptContext);
  }

}
