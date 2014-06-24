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
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.security.CodeSource;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.reflect.ReflectData;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.util.DistCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.kitesdk.compat.Hadoop;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.ValidationException;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.PartitionStrategyParser;
import org.kitesdk.data.spi.SchemaUtil;
import org.kitesdk.data.spi.partition.IdentityFieldPartitioner;

public class TaskUtil {

  /**
   * Returns a configuration builder for the given {@link Job}.
   * @param job a {@code Job} to configure
   */
  public static ConfigBuilder configure(Job job) {
    return new ConfigBuilder(job);
  }

  /**
   * Returns a configuration builder for the given {@link Configuration}.
   * @param conf a {@code Configuration} to configure
   */
  public static ConfigBuilder configure(Configuration conf) {
    return new ConfigBuilder(conf);
  }

  public static class ConfigBuilder {
    private final Configuration conf;
    // this is needed because local distributed cache fails on jar files
    private final boolean skipDistributedCache;

    private ConfigBuilder(Job job) {
      this(Hadoop.JobContext.getConfiguration.<Configuration>invoke(job));
    }

    private ConfigBuilder(Configuration conf) {
      this.conf = conf;
      this.skipDistributedCache = conf.getBoolean("kite.testing", false);
    }

    /**
     * Finds the jar that contains the required class and adds it to the
     * distributed cache configuration.
     *
     * @param requiredClass a class required for a MR job
     * @return this for method chaining
     */
    public ConfigBuilder addJarForClass(Class<?> requiredClass) {
      if (!skipDistributedCache) {
        File jar = findJarForClass(requiredClass);
        try {
          DistCache.addJarToDistributedCache(conf, jar);
        } catch (IOException e) {
          throw new DatasetIOException(
              "Cannot add jar to distributed cache: " + jar, e);
        }
      }
      return this;
    }

    /**
     * Finds the jar that contains the required class and adds its containing
     * directory to the distributed cache configuration.
     *
     * @param requiredClass a class required for a MR job
     * @return this for method chaining
     */
    public ConfigBuilder addJarPathForClass(Class<?> requiredClass) {
      if (!skipDistributedCache) {
        String jarPath = findJarForClass(requiredClass).getParent();
        try {
          DistCache.addJarDirToDistributedCache(conf, jarPath);
        } catch (IOException e) {
          throw new DatasetIOException(
              "Cannot add jar path to distributed cache: " + jarPath, e);
        }
      }
      return this;
    }
  }

  private static File findJarForClass(Class<?> requiredClass) {
    ProtectionDomain domain = AccessController.doPrivileged(
        new GetProtectionDomain(requiredClass));
    CodeSource codeSource = domain.getCodeSource();
    if (codeSource != null) {
      try {
        return new File(codeSource.getLocation().toURI());
      } catch (URISyntaxException e) {
        throw new DatasetException(
            "Cannot locate " + requiredClass.getName() + " jar", e);
      }
    } else {
      // this should only happen for system classes
      throw new DatasetException(
          "Cannot locate " + requiredClass.getName() + " jar");
    }
  }

  /**
   * A PrivilegedAction that gets the ProtectionDomain for a dependency class.
   *
   * Using a PrivilegedAction to retrieve the domain allows security policies
   * to enable Kite to do this, but exclude client code.
   */
  private static class GetProtectionDomain
      implements PrivilegedAction<ProtectionDomain> {
    private final Class<?> requiredClass;

    public GetProtectionDomain(Class<?> requiredClass) {
      this.requiredClass = requiredClass;
    }

    @Override
    public ProtectionDomain run() {
      return requiredClass.getProtectionDomain();
    }
  }

  public static <E> PCollection<E> partition(PCollection<E> collection,
                                             DatasetDescriptor descriptor,
                                             int numReducers) {
    if (descriptor.isPartitioned()) {
      return partition(collection, descriptor.getPartitionStrategy(),
          descriptor.getSchema(), numReducers);
    } else {
      return partition(collection, numReducers);
    }
  }

  public static <E> PCollection<E> partition(PCollection<E> collection,
                                             PartitionStrategy strategy,
                                             Schema schema, int numReducers) {
    GetStorageKey<E> getKey = new GetStorageKey<E>(strategy, schema);
    PTable<GenericData.Record, E> table = collection
        .by(getKey, Avros.generics(getKey.schema()));
    PGroupedTable<GenericData.Record, E> grouped =
        numReducers > 0 ? table.groupByKey(numReducers) : table.groupByKey();
    return grouped.ungroup().values();
  }

  public static <E> PCollection<E> partition(PCollection<E> collection,
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
  public static class AsKeyTable<E> extends DoFn<E, Pair<E, Void>> {
    @Override
    public void process(E entity, Emitter<Pair<E, Void>> emitter) {
      emitter.emit(Pair.of(entity, (Void) null));
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="SE_NO_SERIALVERSIONID",
      justification="Purposely not supported across versions")
  public static class GetStorageKey<E> extends MapFn<E, GenericData.Record> {
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
  public static class AvroStorageKey extends GenericData.Record {
    private final PartitionStrategy strategy;
    private final Schema schema;

    private AvroStorageKey(PartitionStrategy strategy, Schema schema) {
      super(keySchema(strategy, schema));
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

    private static Schema keySchema(PartitionStrategy strategy, Schema schema) {
      List<Schema.Field> partitions = new ArrayList<Schema.Field>();
      for (FieldPartitioner<?, ?> fp : strategy.getFieldPartitioners()) {
        Schema fieldSchema;
        if (fp instanceof IdentityFieldPartitioner) {
          // copy the schema directly from the entity to preserve annotations
          fieldSchema = schema
              .getField(fp.getSourceName()).schema();
        } else {
          Class<?> fieldType = SchemaUtil
              .getPartitionType(fp, schema);
          if (fieldType == Integer.class) {
            fieldSchema = Schema.create(Schema.Type.INT);
          } else if (fieldType == Long.class) {
            fieldSchema = Schema.create(Schema.Type.LONG);
          } else if (fieldType == String.class) {
            fieldSchema = Schema.create(Schema.Type.STRING);
          } else {
            throw new ValidationException(
                "Cannot encode partition " + fp.getName() +
                    " with type " + fp.getSourceType()
            );
          }
        }
        partitions.add(new Schema.Field(
            fp.getName(), fieldSchema, null, null));
      }
      Schema keySchema = Schema.createRecord(
          schema.getName() + "AvroKeySchema", null, null, false);
      keySchema.setFields(partitions);
      return keySchema;
    }
  }
}
