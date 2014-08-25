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
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.kitesdk.compat.Hadoop;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.spi.PartitionKey;
import org.kitesdk.data.spi.PartitionedDataset;
import org.kitesdk.data.TypeNotFoundException;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.InputFormatAccessor;
import org.kitesdk.data.spi.filesystem.FileSystemDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOG =
      LoggerFactory.getLogger(DatasetKeyInputFormat.class);

  public static final String KITE_INPUT_URI = "kite.inputUri";
  public static final String KITE_PARTITION_DIR = "kite.inputPartitionDir";
  public static final String KITE_TYPE = "kite.inputEntityType";

  private Configuration conf;
  private InputFormat<E, Void> delegate;

  public static class ConfigBuilder {
    private final Configuration conf;

    private ConfigBuilder(Job job) {
      this.conf = Hadoop.JobContext.getConfiguration.invoke(job);
    }

    private ConfigBuilder(Configuration conf) {
      this.conf = conf;
    }

    /**
     * Adds configuration for {@code DatasetKeyInputFormat} to read from the
     * given dataset or view URI.
     * <p>
     * URI formats are defined by {@link Dataset} implementations, but must
     * begin with "dataset:" or "view:". For more information, see
     * {@link Datasets}.
     *
     * @param uri a dataset or view URI
     * @return this for method chaining
     */
    public ConfigBuilder readFrom(URI uri) {
      conf.set(KITE_INPUT_URI, uri.toString());
      return this;
    }

    /**
     * Adds configuration for {@code DatasetKeyInputFormat} to read from the
     * given {@link Dataset} or {@link View} instance.
     *
     * @param view a dataset or view
     * @return this for method chaining
     */
    public ConfigBuilder readFrom(View<?> view) {
      if (view instanceof FileSystemDataset) {
        FileSystemDataset dataset = (FileSystemDataset) view;
        conf.set(KITE_PARTITION_DIR,
            String.valueOf(dataset.getDescriptor().getLocation()));
      }
      withType(view.getType());
      return readFrom(view.getUri());
    }

    /**
     * Adds configuration for {@code DatasetKeyInputFormat} to read from the
     * given dataset or view URI string.
     * <p>
     * URI formats are defined by {@link Dataset} implementations, but must
     * begin with "dataset:" or "view:". For more information, see
     * {@link Datasets}.
     *
     * @param uri a dataset or view URI string
     * @return this for method chaining
     */
    public ConfigBuilder readFrom(String uri) {
      return readFrom(URI.create(uri));
    }

    /**
     * Sets the entity Class that the input Dataset should produce.
     * <p>
     * This Class is used to configure the input {@code Dataset}. If this class
     * cannot be found during job setup, the job will fail and throw a
     * {@link org.kitesdk.data.TypeNotFoundException}.
     *
     * @param type the entity Class that will be produced
     * @return this for method chaining
     */
    public <E> ConfigBuilder withType(Class<E> type) {
      conf.setClass(KITE_TYPE, type, type);
      return this;
    }
  }

  /**
   * Configures the {@code Job} to use the {@code DatasetKeyInputFormat} and
   * returns a helper to add further configuration.
   *
   * @param job the {@code Job} to configure
   *
   * @since 0.15.0
   */
  public static ConfigBuilder configure(Job job) {
    job.setInputFormatClass(DatasetKeyInputFormat.class);
    return new ConfigBuilder(job);
  }

  /**
   * Returns a helper to add input options to the given {@code Configuration}.
   *
   * @param conf a {@code Configuration}
   *
   * @since 0.15.0
   */
  public static ConfigBuilder configure(Configuration conf) {
    return new ConfigBuilder(conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration configuration) {
    conf = configuration;
    View<E> view = load(configuration);

    String partitionDir = conf.get(KITE_PARTITION_DIR);
    if (view.getDataset().getDescriptor().isPartitioned() && partitionDir != null) {
      delegate = getDelegateInputFormatForPartition(view.getDataset(), partitionDir, conf);
    } else {
      delegate = getDelegateInputFormat(view, conf);
    }
  }

  @SuppressWarnings("unchecked")
  private InputFormat<E, Void> getDelegateInputFormat(View<E> view, Configuration conf) {
    if (view instanceof InputFormatAccessor) {
      return ((InputFormatAccessor<E>) view).getInputFormat(conf);
    }
    throw new UnsupportedOperationException("Implementation " +
          "does not provide InputFormat support. View: " + view);
  }

  private InputFormat<E, Void> getDelegateInputFormatForPartition(Dataset<E> dataset,
      String partitionDir, Configuration conf) {
    if (!(dataset instanceof FileSystemDataset)) {
      throw new UnsupportedOperationException("Partitions only supported for " +
          "FileSystemDataset. Dataset: " + dataset);
    }
    FileSystemDataset<E> fsDataset = (FileSystemDataset<E>) dataset;
    LOG.debug("Getting delegate input format for dataset {} with partition directory {}",
        dataset, partitionDir);
    PartitionKey key = fsDataset.keyFromDirectory(new Path(partitionDir));
    LOG.debug("Partition key: {}", key);
    if (key != null) {
      PartitionedDataset<E> partition = fsDataset.getPartition(key, false);
      LOG.debug("Partition: {}", partition);
      return getDelegateInputFormat(partition, conf);
    }
    throw new DatasetException("Cannot find partition " + partitionDir);
  }

  @SuppressWarnings({"deprecation", "unchecked"})
  private static <E> View<E> load(Configuration conf) {
    Class<E> type; 
    try {
      type = (Class<E>)conf.getClass(KITE_TYPE, GenericData.Record.class);
    } catch (RuntimeException e) {
      if (e.getCause() instanceof ClassNotFoundException) {
        throw new TypeNotFoundException(String.format(
            "The Java class %s for the entity type could not be found",
            conf.get(KITE_TYPE)),
            e.getCause());
      } else {
        throw e;
      }
    }
    String inputUri = conf.get(KITE_INPUT_URI);
    return Datasets.<E, View<E>>load(inputUri, type);
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
