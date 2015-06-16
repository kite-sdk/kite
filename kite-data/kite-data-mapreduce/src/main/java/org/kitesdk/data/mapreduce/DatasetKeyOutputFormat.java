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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.kitesdk.compat.Hadoop;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.Signalable;
import org.kitesdk.data.TypeNotFoundException;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.AbstractDataset;
import org.kitesdk.data.spi.Compatibility;
import org.kitesdk.data.spi.Constraints;
import org.kitesdk.data.spi.DataModelUtil;
import org.kitesdk.data.spi.DatasetRepositories;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.DefaultConfiguration;
import org.kitesdk.data.spi.Mergeable;
import org.kitesdk.data.spi.PartitionKey;
import org.kitesdk.data.spi.Registration;
import org.kitesdk.data.spi.Replaceable;
import org.kitesdk.data.spi.TemporaryDatasetRepository;
import org.kitesdk.data.spi.TemporaryDatasetRepositoryAccessor;
import org.kitesdk.data.spi.filesystem.FileSystemDataset;
import org.kitesdk.data.spi.filesystem.FileSystemProperties;

/**
 * A MapReduce {@code OutputFormat} for writing to a {@link Dataset}.
 *
 * Since a {@code Dataset} only contains entities (not key/value pairs), this output
 * format ignores the value.
 *
 * @param <E> The type of entities in the {@code Dataset}.
 */
public class DatasetKeyOutputFormat<E> extends OutputFormat<E, Void> {

  public static final String KITE_OUTPUT_URI = "kite.outputUri";
  public static final String KITE_PARTITION_DIR = "kite.outputPartitionDir";
  public static final String KITE_TYPE = "kite.outputEntityType";
  public static final String KITE_WRITE_MODE = "kite.outputMode";
  public static final String KITE_COPY_RECORDS = "kite.copyOutputRecords";

  public enum WriteMode {
    DEFAULT, APPEND, OVERWRITE
  }

  private static final String TEMP_NAMESPACE = "mr";

  public static class ConfigBuilder {
    private final Configuration conf;

    private ConfigBuilder(Job job) {
      this(Hadoop.JobContext.getConfiguration.<Configuration>invoke(job));
    }

    private ConfigBuilder(Configuration conf) {
      this.conf = conf;
      // always use the new API for OutputCommitters, even for 0 reducers
      conf.setBoolean("mapred.reducer.new-api", true);
    }

    /**
     * Adds configuration for {@code DatasetKeyOutputFormat} to write to the
     * given dataset or view URI.
     * <p>
     * URI formats are defined by {@link Dataset} implementations, but must
     * begin with "dataset:" or "view:". For more information, see
     * {@link Datasets}.
     *
     * @param uri a dataset or view URI
     * @return this for method chaining
     */
    public ConfigBuilder writeTo(URI uri) {
      conf.set(KITE_OUTPUT_URI, uri.toString());
      return this;
    }

    /**
     * Adds configuration for {@code DatasetKeyOutputFormat} to write to the
     * given dataset or view URI after removing any existing data.
     * <p>
     * The underlying dataset implementation must support View#deleteAll for
     * the view identified by the URI or the job will fail.
     * <p>
     * URI formats are defined by {@link Dataset} implementations, but must
     * begin with "dataset:" or "view:". For more information, see
     * {@link Datasets}.
     *
     * @param uri a dataset or view URI
     * @return this for method chaining
     *
     * @since 0.16.0
     */
    public ConfigBuilder overwrite(URI uri) {
      setOverwrite();
      return writeTo(uri);
    }

    /**
     * Adds configuration for {@code DatasetKeyOutputFormat} to append to the
     * given dataset or view URI, leaving any existing data intact.
     * <p>
     * URI formats are defined by {@link Dataset} implementations, but must
     * begin with "dataset:" or "view:". For more information, see
     * {@link Datasets}.
     *
     * @param uri a dataset or view URI
     * @return this for method chaining
     *
     * @since 0.16.0
     */
    public ConfigBuilder appendTo(URI uri) {
      setAppend();
      return writeTo(uri);
    }

    /**
     * Adds configuration for {@code DatasetKeyOutputFormat} to write to the
     * given {@link Dataset} or {@link View} instance.
     *
     * @param view a dataset or view
     * @return this for method chaining
     */
    public ConfigBuilder writeTo(View<?> view) {
      if (view instanceof FileSystemDataset) {
        FileSystemDataset dataset = (FileSystemDataset) view;
        conf.set(KITE_PARTITION_DIR,
            String.valueOf(dataset.getDescriptor().getLocation()));
      }
      withType(view.getType());
      return writeTo(view.getUri());
    }

    /**
     * Adds configuration for {@code DatasetKeyOutputFormat} to write to the
     * given {@link Dataset} or {@link View} instance after removing any
     * existing data.
     * <p>
     * The underlying dataset implementation must support View#deleteAll for
     * the {@code view} or the job will fail.
     *
     * @param view a dataset or view
     * @return this for method chaining
     *
     * @since 0.16.0
     */
    public ConfigBuilder overwrite(View<?> view) {
      setOverwrite();
      return writeTo(view);
    }

    /**
     * Adds configuration for {@code DatasetKeyOutputFormat} to append to the
     * given dataset or view URI, leaving any existing data intact.
     *
     * @param view a dataset or view
     * @return this for method chaining
     *
     * @since 0.16.0
     */
    public ConfigBuilder appendTo(View<?> view) {
      setAppend();
      return writeTo(view);
    }

    /**
     * Adds configuration for {@code DatasetKeyOutputFormat} to write to the
     * given dataset or view URI string.
     * <p>
     * URI formats are defined by {@link Dataset} implementations, but must
     * begin with "dataset:" or "view:". For more information, see
     * {@link Datasets}.
     *
     * @param uri a dataset or view URI string
     * @return this for method chaining
     */
    public ConfigBuilder writeTo(String uri) {
      return writeTo(URI.create(uri));
    }

    /**
     * Adds configuration for {@code DatasetKeyOutputFormat} to write to the
     * given dataset or view URI string after removing any existing data.
     * <p>
     * The underlying dataset implementation must support View#deleteAll for
     * the view identified by the URI string or the job will fail.
     * <p>
     * URI formats are defined by {@link Dataset} implementations, but must
     * begin with "dataset:" or "view:". For more information, see
     * {@link Datasets}.
     *
     * @param uri a dataset or view URI string
     * @return this for method chaining
     *
     * @since 0.16.0
     */
    public ConfigBuilder overwrite(String uri) {
      setOverwrite();
      return writeTo(uri);
    }

    /**
     * Adds configuration for {@code DatasetKeyOutputFormat} to append to the
     * given dataset or view URI, leaving any existing data intact.
     * <p>
     * URI formats are defined by {@link Dataset} implementations, but must
     * begin with "dataset:" or "view:". For more information, see
     * {@link Datasets}.
     *
     * @param uri a dataset or view URI string
     * @return this for method chaining
     *
     * @since 0.16.0
     */
    public ConfigBuilder appendTo(String uri) {
      setAppend();
      return writeTo(uri);
    }

    /**
     * Sets the entity Class that will be output by the Job.
     * <p>
     * This Class is used to configure the output {@code Dataset}.
     *
     * @param type the entity Class that will be produced
     * @return this for method chaining
     */
    public <E> ConfigBuilder withType(Class<E> type) {
      conf.setClass(KITE_TYPE, type, type);
      return this;
    }

    private void setOverwrite() {
      String mode = conf.get(KITE_WRITE_MODE);
      Preconditions.checkState(mode == null,
          "Cannot replace existing write mode: " + mode);
      conf.setEnum(KITE_WRITE_MODE, WriteMode.OVERWRITE);
    }

    private void setAppend() {
      String mode = conf.get(KITE_WRITE_MODE);
      Preconditions.checkState(mode == null,
          "Cannot replace existing write mode: " + mode);
      conf.setEnum(KITE_WRITE_MODE, WriteMode.APPEND);
    }
  }

  /**
   * Configures the {@code Job} to use the {@code DatasetKeyOutputFormat} and
   * returns a helper to add further configuration.
   *
   * @param job the {@code Job} to configure
   *
   * @since 0.15.0
   */
  public static ConfigBuilder configure(Job job) {
    job.setOutputFormatClass(DatasetKeyOutputFormat.class);
    return new ConfigBuilder(job);
  }

  /**
   * Returns a helper to add output options to the given {@code Configuration}.
   *
   * @param conf a {@code Configuration}
   *
   * @since 0.15.0
   */
  public static ConfigBuilder configure(Configuration conf) {
    return new ConfigBuilder(conf);
  }

  static class DatasetRecordWriter<E> extends RecordWriter<E, Void> {

    private DatasetWriter<E> datasetWriter;
    private GenericData dataModel;
    private boolean copyRecords;
    private Schema schema;

    public DatasetRecordWriter(View<E> view, boolean copyRecords) {
      this.datasetWriter = view.newWriter();

      this.schema = view.getDataset().getDescriptor().getSchema();
      this.dataModel = DataModelUtil.getDataModelForType(
          view.getType());
      this.copyRecords = copyRecords;
    }

    @Override
    public void write(E key, Void v) {
      if (copyRecords) {
        key = copy(key);
      }
      datasetWriter.write(key);
    }

    private <E> E copy(E key) {
      return dataModel.deepCopy(schema, key);
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) {
      datasetWriter.close();
    }
  }

  static class NullOutputCommitter<E> extends OutputCommitter {
    @Override
    public void setupJob(JobContext jobContext) { }

    @Override
    public void commitJob(JobContext jobContext) throws IOException {
      View<E> targetView = load(jobContext);

      if (targetView instanceof Signalable) {
        ((Signalable)targetView).signalReady();
      }
    }

    @Override
    public void setupTask(TaskAttemptContext taskContext) { }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext) {
      return false;
    }

    @Override
    public void commitTask(TaskAttemptContext taskContext) { }

    @Override
    public void abortTask(TaskAttemptContext taskContext) { }
  }

  static class MergeOutputCommitter<E> extends OutputCommitter {
    @Override
    public void setupJob(JobContext jobContext) {
      loadOrCreateJobDataset(jobContext);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void commitJob(JobContext jobContext) throws IOException {
      Configuration conf = Hadoop.JobContext
          .getConfiguration.invoke(jobContext);
      DatasetRepository repo = getDatasetRepository(jobContext);
      boolean isTemp = repo instanceof TemporaryDatasetRepository;

      String jobDatasetName = getJobDatasetName(jobContext);
      View<E> targetView = load(jobContext);
      Dataset<E> jobDataset = repo.load(TEMP_NAMESPACE, jobDatasetName);
      WriteMode mode = conf.getEnum(KITE_WRITE_MODE, WriteMode.DEFAULT);
      if (mode == WriteMode.OVERWRITE && canReplace(targetView)) {
        ((Replaceable<View<E>>) targetView.getDataset()).replace(targetView, jobDataset);
      } else {
        ((Mergeable<Dataset<E>>) targetView.getDataset()).merge(jobDataset);
      }

      if (targetView instanceof Signalable) {
        ((Signalable)targetView).signalReady();
      }

      if (isTemp) {
        ((TemporaryDatasetRepository) repo).delete();
      } else {
        repo.delete(TEMP_NAMESPACE, jobDatasetName);
      }
    }

    @Override
    public void abortJob(JobContext jobContext, JobStatus.State state) {
      deleteJobDataset(jobContext);
    }

    @Override
    public void setupTask(TaskAttemptContext taskContext) {
      // do nothing: the task attempt dataset is created in getRecordWriter
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext) {
      return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void commitTask(TaskAttemptContext taskContext) throws IOException {
      DatasetRepository repo = getDatasetRepository(taskContext);
      boolean inTempRepo = repo instanceof TemporaryDatasetRepository;

      Dataset<E> jobDataset = repo.load(TEMP_NAMESPACE, getJobDatasetName(taskContext));
      String taskAttemptDatasetName = getTaskAttemptDatasetName(taskContext);
      if (repo.exists(TEMP_NAMESPACE, taskAttemptDatasetName)) {
        Dataset<E> taskAttemptDataset = repo.load(TEMP_NAMESPACE, taskAttemptDatasetName);
        ((Mergeable<Dataset<E>>) jobDataset).merge(taskAttemptDataset);
        if (!inTempRepo) {
          repo.delete(TEMP_NAMESPACE, taskAttemptDatasetName);
        }
      }
    }

    @Override
    public void abortTask(TaskAttemptContext taskContext) {
      deleteTaskAttemptDataset(taskContext);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public RecordWriter<E, Void> getRecordWriter(TaskAttemptContext taskAttemptContext) {
    Configuration conf = Hadoop.TaskAttemptContext
        .getConfiguration.invoke(taskAttemptContext);
    View<E> target = load(taskAttemptContext);
    View<E> working;

    if (usePerTaskAttemptDatasets(target)) {
      working = loadOrCreateTaskAttemptView(taskAttemptContext);
    } else {
      working = target;
    }

    boolean copyRecords = conf.getBoolean(KITE_COPY_RECORDS, false);

    String partitionDir = conf.get(KITE_PARTITION_DIR);
    if (working.getDataset().getDescriptor().isPartitioned() &&
        partitionDir != null) {
      if (!(target instanceof FileSystemDataset)) {
        throw new UnsupportedOperationException("Partitions only supported for " +
            "FileSystemDataset. Dataset: " + target);
      }
      FileSystemDataset fsDataset = (FileSystemDataset) target;
      PartitionKey key = fsDataset.keyFromDirectory(new Path(partitionDir));
      if (key != null && !key.getValues().isEmpty()) {
        working = fsDataset.getPartition(key, true);
      }
      return new DatasetRecordWriter<E>(working, copyRecords);
    } else {
      return new DatasetRecordWriter<E>(working, copyRecords);
    }
  }

  @Override
  public void checkOutputSpecs(JobContext jobContext) {
    // The committer setup will fail if the output dataset does not exist
    View<E> target = load(jobContext);
    Configuration conf = Hadoop.JobContext.getConfiguration.invoke(jobContext);
    switch (conf.getEnum(KITE_WRITE_MODE, WriteMode.DEFAULT)) {
      case APPEND:
        break;
      case OVERWRITE:
        // if the merge won't use replace, then delete the existing data
        if (!canReplace(target)) {
          target.deleteAll();
        }
        break;
      default:
      case DEFAULT:
        boolean isReady = false;
        if (target instanceof Signalable) {
          isReady = ((Signalable)target).isReady();
        }
        if (isReady || !target.isEmpty()) {
          throw new DatasetException(
              "View is not empty or has been signaled as ready: " + target);
        }
        break;
    }
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) {
    Configuration conf = Hadoop.TaskAttemptContext.getConfiguration.invoke(taskAttemptContext);
    DefaultConfiguration.init(conf);
    View<E> view = load(taskAttemptContext);
    return usePerTaskAttemptDatasets(view) ?
        new MergeOutputCommitter<E>() : new NullOutputCommitter();
  }

  private static <E> boolean usePerTaskAttemptDatasets(View<E> target) {
    // new API output committers are not called properly in Hadoop 1
    return !Hadoop.isHadoop1() && target.getDataset() instanceof Mergeable;
  }

  // TODO: Remove the need to use DatasetRepositories.repositoryFor()
  private static DatasetRepository getDatasetRepository(JobContext jobContext) {
    Configuration conf = Hadoop.JobContext.getConfiguration.invoke(jobContext);
    DatasetRepository repo = DatasetRepositories.repositoryFor(conf.get(KITE_OUTPUT_URI));
    if (repo instanceof TemporaryDatasetRepositoryAccessor) {
      Dataset<Object> dataset = load(jobContext).getDataset();
      String namespace = dataset.getNamespace();
      repo = ((TemporaryDatasetRepositoryAccessor) repo)
          .getTemporaryRepository(namespace, getJobDatasetName(jobContext));
    }
    return repo;
  }

  private static String getJobDatasetName(JobContext jobContext) {
    return Hadoop.JobContext.getJobID.invoke(jobContext).toString();
  }

  private static String getTaskAttemptDatasetName(TaskAttemptContext taskContext) {
    return taskContext.getTaskAttemptID().toString();
  }

  @SuppressWarnings("deprecation")
  private static <E> View<E> load(JobContext jobContext) {
    Configuration conf = Hadoop.JobContext.getConfiguration.invoke(jobContext);
    Class<E> type = getType(jobContext);

    String outputUri = conf.get(KITE_OUTPUT_URI);
    return Datasets.<E, View<E>>load(outputUri, type);
  }

  @SuppressWarnings("unchecked")
  private static <E> Class<E> getType(JobContext jobContext) {
    Configuration conf = Hadoop.JobContext.getConfiguration.invoke(jobContext);
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
    return type;
  }

  /**
   * The job dataset may already exist if the ApplicationMaster was restarted
   */
  @SuppressWarnings("unchecked")
  private static <E> Dataset<E> loadOrCreateJobDataset(JobContext jobContext) {
    Dataset<Object> dataset = load(jobContext).getDataset();
    String jobDatasetName = getJobDatasetName(jobContext);
    DatasetRepository repo = getDatasetRepository(jobContext);
    if (repo.exists(TEMP_NAMESPACE, jobDatasetName)) {
      Dataset<E> tempDataset = repo.load(TEMP_NAMESPACE, jobDatasetName,
        DatasetKeyOutputFormat.<E>getType(jobContext));
      try {
        Compatibility.checkCompatible(dataset.getDescriptor(),
          tempDataset.getDescriptor());
        return tempDataset;
      } catch (RuntimeException ex) {
        // swallow
      }
    }

    return repo.create(TEMP_NAMESPACE, jobDatasetName,
        copy(dataset.getDescriptor()),
        DatasetKeyOutputFormat.<E>getType(jobContext));
  }

  private static <E> Dataset<E> loadJobDataset(JobContext jobContext) {
    DatasetRepository repo = getDatasetRepository(jobContext);
    return repo.load(TEMP_NAMESPACE, getJobDatasetName(jobContext));
  }

  private static void deleteJobDataset(JobContext jobContext) {
    DatasetRepository repo = getDatasetRepository(jobContext);
    repo.delete(TEMP_NAMESPACE, getJobDatasetName(jobContext));
  }

  private static <E> Dataset<E> loadOrCreateTaskAttemptDataset(TaskAttemptContext taskContext) {
    String taskAttemptDatasetName = getTaskAttemptDatasetName(taskContext);
    DatasetRepository repo = getDatasetRepository(taskContext);
    Dataset<E> jobDataset = loadJobDataset(taskContext);
    if (repo.exists(TEMP_NAMESPACE, taskAttemptDatasetName)) {
      return repo.load(TEMP_NAMESPACE, taskAttemptDatasetName);
    } else {
      return repo.create(TEMP_NAMESPACE, taskAttemptDatasetName,
          copy(jobDataset.getDescriptor()));
    }
  }

  private static <E> View<E> loadOrCreateTaskAttemptView(TaskAttemptContext taskContext) {
    Configuration conf = Hadoop.JobContext.getConfiguration.invoke(taskContext);
    Map<String, String> uriOptions = Registration.lookupDatasetUri(
        URI.create(URI.create(conf.get(KITE_OUTPUT_URI)).getSchemeSpecificPart())).second();
    Dataset<E> dataset = loadOrCreateTaskAttemptDataset(taskContext);

    if (dataset instanceof AbstractDataset) {
      DatasetDescriptor descriptor = dataset.getDescriptor();
      Schema schema = descriptor.getSchema();
      PartitionStrategy strategy = null;
      if (descriptor.isPartitioned()) {
        strategy = descriptor.getPartitionStrategy();
      }
      Constraints constraints = Constraints.fromQueryMap(
          schema, strategy, uriOptions);
      return ((AbstractDataset<E>) dataset).filter(constraints);
    } else {
      return dataset;
    }
  }

  private static void deleteTaskAttemptDataset(TaskAttemptContext taskContext) {
    DatasetRepository repo = getDatasetRepository(taskContext);
    String taskAttemptDatasetName = getTaskAttemptDatasetName(taskContext);
    if (repo.exists(TEMP_NAMESPACE, taskAttemptDatasetName)) {
      repo.delete(TEMP_NAMESPACE, taskAttemptDatasetName);
    }
  }

  private static DatasetDescriptor copy(DatasetDescriptor descriptor) {
    // don't reuse the previous dataset's location and don't use durable
    // parquet writers because fault-tolerance is handled by OutputCommitter
    return new DatasetDescriptor.Builder(descriptor)
        .property(FileSystemProperties.NON_DURABLE_PARQUET_PROP, "true")
        .location((URI) null)
        .build();
  }

  @SuppressWarnings("unchecked")
  private static boolean canReplace(View<?> view) {
    if (Hadoop.isHadoop1()) {
      // can't use replace because it is called in the OutputCommitter.
      return false;
    }
    Dataset<?> dataset = view.getDataset();
    return (dataset instanceof Replaceable &&
        ((Replaceable<View<?>>) dataset).canReplace(view));
  }
}
