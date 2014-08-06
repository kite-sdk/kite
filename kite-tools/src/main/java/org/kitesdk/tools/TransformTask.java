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
import com.google.common.io.Closeables;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.reflect.ReflectData;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.PipelineResult.StageResult;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hive.conf.HiveConf;
import org.kitesdk.compat.DynMethods;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.View;
import org.kitesdk.data.crunch.CrunchDatasets;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.PartitionStrategyParser;
import org.kitesdk.data.spi.SchemaUtil;

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

  private long count = 0;

  public TransformTask(View<S> from, View<T> to, DoFn<S, T> transform) {
    this.from = from;
    this.to = to;
    this.transform = transform;
  }

  public long getCount() {
    return count;
  }

  public TransformTask noCompaction() {
    this.compact = false;
    this.numWriters = 0;
    return this;
  }

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

  public PipelineResult run() throws IOException {
    boolean runInParallel = true;
    if (isLocal(from.getDataset()) || isLocal(to.getDataset())) {
      runInParallel = false;
    }

    PType<T> toPType = ptype(to);
    MapFn<T, T> validate = new CheckEntityClass<T>(to.getType());

    if (runInParallel) {
      TaskUtil.configure(getConf())
          .addJarPathForClass(HiveConf.class)
          .addJarForClass(AvroKeyInputFormat.class);

      Pipeline pipeline = new MRPipeline(getClass(), getConf());

      PCollection<T> collection = pipeline.read(CrunchDatasets.asSource(from))
          .parallelDo(transform, toPType).parallelDo(validate, toPType);

      if (compact) {
        // the transform must be run before partitioning
        collection = partition(collection, to, numWriters);
      }

      pipeline.write(collection, CrunchDatasets.asTarget(to), Target.WriteMode.APPEND);

      PipelineResult result = pipeline.done();

      StageResult sr = Iterables.getFirst(result.getStageResults(), null);
      if (sr != null && MAP_INPUT_RECORDS != null) {
        this.count = sr.getCounterValue(MAP_INPUT_RECORDS);
      }

      return result;

    } else {
      Pipeline pipeline = MemPipeline.getInstance();

      PCollection<T> collection = pipeline.read(CrunchDatasets.asSource(from))
          .parallelDo(transform, toPType).parallelDo(validate, toPType);

      boolean threw = true;
      DatasetWriter<T> writer = null;
      try {
        writer = to.newWriter();

        for (T entity : collection.materialize()) {
          writer.write(entity);
          count += 1;
        }

        threw = false;

      } finally {
        Closeables.close(writer, threw);
      }

      return pipeline.done();
    }
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

  private static <E> PCollection<E> partition(PCollection<E> collection,
                                              View<E> view,
                                              int numReducers) {
    return partition(collection, view.getDataset(), numReducers);
  }

  private static <E> PCollection<E> partition(PCollection<E> collection,
                                              Dataset<E> dataset,
                                              int numReducers) {
    return partition(collection, dataset.getDescriptor(), numReducers);
  }

  private static <E> PCollection<E> partition(PCollection<E> collection,
                                              DatasetDescriptor descriptor,
                                              int numReducers) {
    if (descriptor.isPartitioned()) {
      GetStorageKey<E> getKey = new GetStorageKey<E>(
          descriptor.getPartitionStrategy(), descriptor.getSchema());
      PTable<GenericData.Record, E> table = collection
          .by(getKey, Avros.generics(getKey.schema()));
      PGroupedTable<GenericData.Record, E> grouped =
          numReducers > 0 ? table.groupByKey(numReducers) : table.groupByKey();
      return grouped.ungroup().values();
    } else {
      return partition(collection, numReducers);
    }
  }

  private static <E> PCollection<E> partition(PCollection<E> collection,
                                              int numReducers) {
    PType<E> type = collection.getPType();
    PTableType<E, Void> tableType = Avros.tableOf(type, Avros.nulls());
    PTable<E, Void> table = collection.parallelDo(new AsKeyTable<E>(), tableType);
    PGroupedTable<E, Void> grouped =
        numReducers > 0 ? table.groupByKey(numReducers) : table.groupByKey();
    return grouped.ungroup().keys();
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="SE_NO_SERIALVERSIONID",
      justification="Purposely not supported across versions")
  private static class AsKeyTable<E> extends DoFn<E, Pair<E, Void>> {
    @Override
    public void process(E entity, Emitter<Pair<E, Void>> emitter) {
      emitter.emit(Pair.of(entity, (Void) null));
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="SE_NO_SERIALVERSIONID",
      justification="Purposely not supported across versions")
  private static class GetStorageKey<E> extends MapFn<E, GenericData.Record> {
    private final String strategyString;
    private final String schemaString;
    private transient AvroStorageKey key = null;

    private GetStorageKey(PartitionStrategy strategy, Schema schema) {
      this.strategyString = strategy.toString(false /* no white space */);
      this.schemaString = schema.toString(false /* no white space */);
    }

    public Schema schema() {
      initialize(); // make sure the key is not null
      return key.getSchema();
    }

    @Override
    public void initialize() {
      if (key == null) {
        PartitionStrategy strategy = PartitionStrategyParser.parse(strategyString);
        Schema schema = new Schema.Parser().parse(schemaString);
        this.key = new AvroStorageKey(strategy, schema);
      }
    }

    @Override
    public AvroStorageKey map(E entity) {
      return key.reuseFor(entity);
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="EQ_DOESNT_OVERRIDE_EQUALS",
      justification="StorageKey equals is correct, compares the values")
  private static class AvroStorageKey extends GenericData.Record {
    private final PartitionStrategy strategy;
    private final Schema schema;

    private AvroStorageKey(PartitionStrategy strategy, Schema schema) {
      super(SchemaUtil.keySchema(schema, strategy));
      this.strategy = strategy;
      this.schema = schema;
    }

    @SuppressWarnings("unchecked")
    public AvroStorageKey reuseFor(Object entity) {
      List<FieldPartitioner> partitioners = strategy.getFieldPartitioners();

      for (int i = 0; i < partitioners.size(); i++) {
        FieldPartitioner fp = partitioners.get(i);
        Schema.Field field = schema.getField(fp.getSourceName());
        // TODO: this should use the correct Avro data model, not just reflect
        Object value = ReflectData.get().getField(
            entity, field.name(), field.pos());
        put(i, fp.apply(value));
      }

      return this;
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
}
