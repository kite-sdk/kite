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
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.net.URI;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
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
import org.kitesdk.data.TypeNotFoundException;
import org.kitesdk.data.URIBuilder;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.DataModelUtil;
import org.kitesdk.data.spi.DatasetRepositories;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.DescriptorUtil;
import org.kitesdk.data.spi.Mergeable;
import org.kitesdk.data.spi.TemporaryDatasetRepository;
import org.kitesdk.data.spi.TemporaryDatasetRepositoryAccessor;
import org.kitesdk.data.spi.filesystem.FileSystemDataset;
import org.kitesdk.data.spi.filesystem.FileSystemDatasets;
import org.kitesdk.data.spi.filesystem.FileSystemProperties;

/**
 * A MapReduce {@code OutputFormat} for writing to a {@link Dataset}.
 *
 * Since a {@code Dataset} only contains entities (not key/value pairs), this output
 * format ignores the value.
 *
 * @param <E> The type of entities in the {@code Dataset}.
 */
@Beta
public class DatasetKeyOutputFormat<E> extends OutputFormat<E, Void> {

  public static final String KITE_OUTPUT_URI = "kite.outputUri";
  public static final String KITE_PARTITION_DIR = "kite.outputPartitionDir";
  public static final String KITE_TYPE = "kite.outputEntityType";

  private static final String KITE_WRITE_MODE = "kite.outputMode";
  private static final String OUT_CONTEXT_NAME = "output";
  private static final String TEMP_NAMESPACE = "mr";

  private static enum WriteMode {
    DEFAULT, APPEND, OVERWRITE
  }

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
      if (Datasets.exists(toDatasetUri(uri.toString()))) {
        withDescriptor(Datasets.load(uri).getDataset().getDescriptor());
      }
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
      DatasetDescriptor descriptor = view.getDataset().getDescriptor();
      URI uri = view.getUri();

      if (view instanceof FileSystemDataset) {
        // the view is actually a partition dataset. convert it to a real view.
        FileSystemDataset dataset = (FileSystemDataset) view;
        Dataset<GenericRecord> rootDataset = Datasets.load(dataset.getUri());
        View<GenericRecord> realView = FileSystemDatasets.viewForPath(
            rootDataset, new Path(dataset.getDescriptor().getLocation()));
        descriptor = rootDataset.getDescriptor();
        uri = realView.getUri();
      }

      withType(view.getType());
      withDescriptor(descriptor);
      return writeTo(uri);
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
     * This Class is used to configure the output {@code Dataset}. If not set,
     * the output type will be {@link GenericRecord}.
     *
     * @param type the entity Class that will be produced
     * @return this for method chaining
     */
    public <E> ConfigBuilder withType(Class<E> type) {
      conf.setClass(KITE_TYPE, type, type);
      return this;
    }

    /**
     * Sets the {@link DatasetDescriptor} for the Job output.
     * <p>
     * This is only used if the output dataset does not exist and must be
     * created by the output format.
     *
     * @param descriptor the DatasetDescriptor for the output dataset.
     * @return this for method chaining
     *
     * @since 0.17.0
     */
    public ConfigBuilder withDescriptor(@Nullable DatasetDescriptor descriptor) {
      if (descriptor != null) {
        DescriptorUtil.addToConfiguration(descriptor, OUT_CONTEXT_NAME, conf);
      }
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
    private boolean copyEntities;
    private Schema schema;

    public DatasetRecordWriter(View<E> view) {
      this.datasetWriter = view.newWriter();

      this.schema = view.getDataset().getDescriptor().getSchema();
      this.dataModel = DataModelUtil.getDataModelForType(
          view.getType());
      if (dataModel.getClass() != ReflectData.class) {
        copyEntities = true;
      }
    }

    @Override
    public void write(E key, Void v) {
      if (copyEntities) {
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

  static class NullOutputCommitter extends OutputCommitter {
    @Override
    public void setupJob(JobContext jobContext) throws IOException { }

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
    public void setupJob(JobContext jobContext) throws IOException {
      // the output dataset is created in commitJob if it doesn't exist
      createJobDataset(jobContext);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void commitJob(JobContext jobContext) throws IOException {
      Configuration conf = getConf(jobContext);
      DatasetRepository repo = getDatasetRepository(jobContext);

      String outputUri = conf.get(KITE_OUTPUT_URI);
      Class<E> type = getType(conf);
      View<E> targetView;
      if (!Datasets.exists(toDatasetUri(outputUri))) {
        // if the Dataset doesn't exist, create it
        targetView = Datasets.create(outputUri,
            DescriptorUtil.buildFromConfiguration(conf, OUT_CONTEXT_NAME),
            type);
      } else {
        targetView = Datasets.load(outputUri, type);
      }

      String jobDatasetName = getJobDatasetName(jobContext);
      Dataset<E> jobDataset = repo.load(TEMP_NAMESPACE, jobDatasetName);
      ((Mergeable<Dataset<E>>) targetView.getDataset()).merge(jobDataset);

      if (repo instanceof TemporaryDatasetRepository) {
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
  public RecordWriter<E, Void> getRecordWriter(
      TaskAttemptContext taskAttemptContext) throws IOException {
    Configuration conf = getConf(taskAttemptContext);
    String outputUri = conf.get(KITE_OUTPUT_URI);
    DatasetRepository repo = getDatasetRepository(taskAttemptContext);

    View<E> working;
    if (usePerTaskAttemptDatasets(repo)) {
      working = loadOrCreateTaskAttemptDataset(taskAttemptContext);
    } else {
      Class<E> type = getType(conf);
      working = Datasets.load(outputUri, type);
    }

    return new DatasetRecordWriter<E>(working);
  }

  @Override
  public void checkOutputSpecs(JobContext jobContext) {
    Configuration conf = Hadoop.JobContext.getConfiguration.invoke(jobContext);
    String outputUri = conf.get(KITE_OUTPUT_URI);
    // A new dataset will work for all modes, only check mode if one exists
    if (Datasets.exists(toDatasetUri(outputUri))) {
      View<GenericRecord> target = Datasets.load(outputUri);
      switch (conf.getEnum(KITE_WRITE_MODE, WriteMode.DEFAULT)) {
        case APPEND:
          break;
        case OVERWRITE:
          if (!target.isEmpty()) {
            target.deleteAll();
          }
          break;
        default:
        case DEFAULT:
          if (!target.isEmpty()) {
            throw new DatasetException("View is not empty: " + target);
          }
          break;
      }
    }
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException {
    DatasetRepository repo = getDatasetRepository(taskAttemptContext);
    if (usePerTaskAttemptDatasets(repo)) {
      return new MergeOutputCommitter<E>();
    }

    // Ensure the output dataset exists because it is written to directly
    Configuration conf = getConf(taskAttemptContext);
    String outputUri = conf.get(KITE_OUTPUT_URI);
    if (!Datasets.exists(toDatasetUri(outputUri))) {
      Datasets.create(outputUri,
          DescriptorUtil.buildFromConfiguration(conf, OUT_CONTEXT_NAME));
    }

    return new NullOutputCommitter();
  }

  private static boolean usePerTaskAttemptDatasets(DatasetRepository repo) {
    // new API output committers are not called properly in Hadoop 1
    return !Hadoop.isHadoop1() && repo instanceof TemporaryDatasetRepository;
  }

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
    return Hadoop.TaskAttemptContext.getTaskAttemptID.invoke(taskContext).toString();
  }

  @SuppressWarnings("unchecked")
  private static <E> Class<E> getType(Configuration conf) {
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

  @SuppressWarnings("unchecked")
  private static <E> Dataset<E> createJobDataset(JobContext jobContext) throws IOException {
    Configuration conf = getConf(jobContext);
    String outputUri = conf.get(KITE_OUTPUT_URI);
    DatasetDescriptor outputDescriptor;
    if (Datasets.exists(toDatasetUri(outputUri))) {
      outputDescriptor = Datasets.load(outputUri).getDataset().getDescriptor();
    } else {
      outputDescriptor = DescriptorUtil.buildFromConfiguration(conf, OUT_CONTEXT_NAME);
    }

    String jobDatasetName = getJobDatasetName(jobContext);
    DatasetRepository repo = getDatasetRepository(jobContext);
    return repo.create(TEMP_NAMESPACE, jobDatasetName,
        copyForTemp(outputDescriptor),
        DatasetKeyOutputFormat.<E>getType(conf));
  }

  private static void deleteJobDataset(JobContext jobContext) {
    DatasetRepository repo = getDatasetRepository(jobContext);
    repo.delete(TEMP_NAMESPACE, getJobDatasetName(jobContext));
  }

  private static <E> Dataset<E> loadOrCreateTaskAttemptDataset(
      TaskAttemptContext taskContext) throws IOException {
    String taskAttemptDatasetName = getTaskAttemptDatasetName(taskContext);
    DatasetRepository repo = getDatasetRepository(taskContext);
    if (repo.exists(TEMP_NAMESPACE, taskAttemptDatasetName)) {
      return repo.load(TEMP_NAMESPACE, taskAttemptDatasetName);
    } else {
      DatasetDescriptor descriptor = DescriptorUtil.buildFromConfiguration(
          getConf(taskContext), OUT_CONTEXT_NAME);
      return repo.create(TEMP_NAMESPACE, taskAttemptDatasetName,
          copyForTemp(descriptor));
    }
  }

  private static void deleteTaskAttemptDataset(TaskAttemptContext taskContext) {
    DatasetRepository repo = getDatasetRepository(taskContext);
    String taskAttemptDatasetName = getTaskAttemptDatasetName(taskContext);
    if (repo.exists(TEMP_NAMESPACE, taskAttemptDatasetName)) {
      repo.delete(TEMP_NAMESPACE, taskAttemptDatasetName);
    }
  }

  private static DatasetDescriptor copyForTemp(DatasetDescriptor descriptor) {
    // don't reuse the previous dataset's location and don't use durable
    // parquet writers because fault-tolerance is handled by OutputCommitter
    return new DatasetDescriptor.Builder(descriptor)
        .property(FileSystemProperties.NON_DURABLE_PARQUET_PROP, "true")
        .location((URI) null)
        .build();
  }

  private static URI toDatasetUri(String uriString) {
    URI uri = URI.create(uriString);
    if (URIBuilder.VIEW_SCHEME.equals(uri.getScheme())) {
      return URI.create(
          URIBuilder.DATASET_SCHEME + ":" + uri.getSchemeSpecificPart());
    }
    return uri;
  }

  private static Configuration getConf(JobContext context) {
    return Hadoop.JobContext.getConfiguration.invoke(context);
  }
}
