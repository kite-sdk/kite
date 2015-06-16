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
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.net.URI;
import org.apache.avro.generic.GenericRecord;
import org.apache.crunch.DoFn;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.PipelineResult.StageResult;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.token.Token;
import org.apache.thrift.TException;
import org.kitesdk.compat.DynMethods;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.View;
import org.kitesdk.data.crunch.CrunchDatasets;

/**
 * @since 0.16.0
 */
public class TransformTask<S, T> extends Configured {

  private static DynMethods.StaticMethod getEnumByName =
      new DynMethods.Builder("valueOf")
          .impl("org.apache.hadoop.mapred.Task$Counter", String.class)
          .impl("org.apache.hadoop.mapreduce.TaskCounter", String.class)
          .defaultNoop() // missing is okay, record count will be 0
          .buildStatic();
  private static final Enum<?> MAP_INPUT_RECORDS = getEnumByName
      .invoke("MAP_INPUT_RECORDS");

  private static final String LOCAL_FS_SCHEME = "file";

  private final View<S> from;
  private final View<T> to;
  private final DoFn<S, T> transform;
  private boolean compact = true;
  private int numWriters = -1;
  private int numPartitionWriters = -1;
  private Target.WriteMode mode = Target.WriteMode.APPEND;

  private long count = 0;


  public TransformTask(View<S> from, View<T> to, DoFn<S, T> transform) {
    this.from = from;
    this.to = to;
    this.transform = transform;
  }

  public long getCount() {
    return count;
  }

  /**
   * Do not shuffle data to writers to minimize the number of files. Write from
   * each read task.
   *
   * @return this for method chaining
   */
  public TransformTask noCompaction() {
    this.compact = false;
    this.numWriters = 0;
    this.numPartitionWriters = 0;
    return this;
  }

  /**
   * Set the number of concurrent writer processes.
   * <p>
   * If set to 0, this disables compaction.
   *
   * @param numWriters the number of concurrent writer processes
   * @return this for method chaining
   */
  public TransformTask setNumWriters(int numWriters) {
    Preconditions.checkArgument(numWriters >= 0,
        "Invalid number of reducers: " + numWriters);
    if (numWriters == 0) {
      noCompaction();
    } else {
      this.numWriters = numWriters;
    }
    return this;
  }

  /**
   * Set the number of files to create in each output partition.
   *
   * @param filesPerPartition a target number of files per output partition
   * @return this for method chaining
   * @since 1.1.0
   */
  public TransformTask setFilesPerPartition(int filesPerPartition) {
    Preconditions.checkArgument(filesPerPartition > 0,
        "Invalid number of files per partition: " + filesPerPartition);
    this.numPartitionWriters = filesPerPartition;
    return this;
  }

  /**
   * Set the output write mode: default, overwrite, or append.
   *
   * @param mode the output write mode
   * @return this for method chaining
   */
  public TransformTask setWriteMode(Target.WriteMode mode) {
    Preconditions.checkArgument(mode != Target.WriteMode.CHECKPOINT,
        "Checkpoint is not an allowed write mode");
    this.mode = mode;
    return this;
  }

  public PipelineResult run() throws IOException {
    if (isLocal(from.getDataset()) || isLocal(to.getDataset())) {
      // copy to avoid making changes to the caller's configuration
      Configuration conf = new Configuration(getConf());
      conf.set("mapreduce.framework.name", "local");
      // replace the default FS for Crunch to avoid distributed cache errors.
      // this doesn't affect the source or target, they are fully-qualified.
      conf.set("fs.defaultFS", "file:/");
      conf.set("fs.default.name", "file:/");
      setConf(conf);
    }

    if (isHive(from) || isHive(to)) {
      setConf(addHiveDelegationToken(getConf()));
    }

    PType<T> toPType = ptype(to);
    MapFn<T, T> validate = new CheckEntityClass<T>(to.getType());

    TaskUtil.configure(getConf())
        .addJarPathForClass(HiveConf.class);

    Pipeline pipeline = new MRPipeline(getClass(), getConf());

    PCollection<T> collection = pipeline.read(CrunchDatasets.asSource(from))
        .parallelDo(transform, toPType).parallelDo(validate, toPType);

    if (compact) {
      // the transform must be run before partitioning
      collection = CrunchDatasets.partition(collection, to, numWriters, numPartitionWriters);
    }

    pipeline.write(collection, CrunchDatasets.asTarget(to), mode);

    PipelineResult result = pipeline.done();

    StageResult sr = Iterables.getFirst(result.getStageResults(), null);
    if (sr != null && MAP_INPUT_RECORDS != null) {
      this.count = sr.getCounterValue(MAP_INPUT_RECORDS);
    }

    return result;
  }

  private static boolean isLocal(Dataset<?> dataset) {
    URI location = dataset.getDescriptor().getLocation();
    return (location != null) && LOCAL_FS_SCHEME.equals(location.getScheme());
  }

  @SuppressWarnings("unchecked")
  private static <T> AvroType<T> ptype(View<T> view) {
    Class<T> recordClass = view.getType();
    if (GenericRecord.class.isAssignableFrom(recordClass)) {
      return (AvroType<T>) Avros.generics(
          view.getDataset().getDescriptor().getSchema());
    } else {
      return Avros.records(recordClass);
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="SE_NO_SERIALVERSIONID",
      justification="Purposely not supported across versions")
  public static class CheckEntityClass<E> extends MapFn<E, E> {
    private final Class<?> entityClass;

    public CheckEntityClass(Class<?> entityClass) {
      this.entityClass = entityClass;
    }

    @Override
    public E map(E input) {
      if (input != null && entityClass.isAssignableFrom(input.getClass())) {
        return input;
      } else {
        throw new DatasetException(
            "Object does not match expected type " + entityClass +
            ": " + String.valueOf(input));
      }
    }
  }

  private static boolean isHive(View<?> view) {
    return "hive".equals(
        URI.create(view.getUri().getSchemeSpecificPart()).getScheme());
  }

  private static final Text HIVE_MS_TOKEN_ALIAS = new Text("HIVE_METASTORE_TOKEN");

  private static Configuration addHiveDelegationToken(Configuration conf) {
    // this uses a JobConf because we don't have access to the MR Job
    JobConf jobConf = new JobConf(conf);

    try {
      if (conf.getBoolean(
          HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname, false)) {
        // Need to set up delegation token auth
        HiveMetaStoreClient metaStoreClient = new HiveMetaStoreClient(new HiveConf());
        String hiveTokenStr = metaStoreClient.getDelegationToken("yarn");
        Token<DelegationTokenIdentifier> hiveToken = new Token<DelegationTokenIdentifier>();
        hiveToken.decodeFromUrlString(hiveTokenStr);
        jobConf.getCredentials().addToken(HIVE_MS_TOKEN_ALIAS, hiveToken);
      }

      return jobConf;

    } catch (IOException e) {
      throw new RuntimeException("Unable to obtain Hive delegation token", e);
    } catch (TException e) {
      throw new RuntimeException("Unable to obtain Hive delegation token", e);
    }
  }
}
