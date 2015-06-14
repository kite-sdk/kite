/**
 * Copyright 2013 Cloudera Inc.
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
package org.kitesdk.data.crunch;

import java.net.URI;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Target;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.Avros;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.AbstractRefinableView;
import org.kitesdk.data.spi.Constraints;
import org.kitesdk.data.spi.DataModelUtil;
import org.kitesdk.data.spi.EntityAccessor;
import org.kitesdk.data.spi.PartitionStrategyParser;
import org.kitesdk.data.spi.SchemaUtil;
import org.kitesdk.data.spi.StorageKey;

/**
 * <p>
 * A helper class for exposing {@link Dataset}s and {@link View}s as Crunch
 * {@link ReadableSource}s or {@link Target}s.
 * </p>
 */
public class CrunchDatasets {

  /**
   * Expose the given {@link View} as a Crunch {@link ReadableSource}.
   *
   * @param view the view to read from
   * @param <E>     the type of entity produced by the source
   * @return a {@link ReadableSource} for the view
   *
   * @since 0.14.0
   */
  public static <E> ReadableSource<E> asSource(View<E> view) {
    return new DatasetSourceTarget<E>(view);
  }

  /**
   * Expose the {@link View} or {@link Dataset} represented by the URI
   * as a Crunch {@link ReadableSource}.
   *
   * @param uri the URI of the view or dataset to read from
   * @param type    the Java type of the entities in the dataset
   * @param <E>     the type of entity produced by the source
   * @return a {@link ReadableSource} for the view
   *
   * @since 0.15.0
   */
  public static <E> ReadableSource<E> asSource(URI uri, Class<E> type) {
    return new DatasetSourceTarget<E>(uri, type);
  }

  /**
   * Expose the {@link View} or {@link Dataset} represented by the URI
   * as a Crunch {@link ReadableSource}.
   *
   * @param uri the URI of the view or dataset to read from
   * @param type    the Java type of the entities in the dataset
   * @param <E>     the type of entity produced by the source
   * @return a {@link ReadableSource} for the view
   *
   * @since 0.15.0
   */
  public static <E> ReadableSource<E> asSource(String uri, Class<E> type) {
    return asSource(URI.create(uri), type);
  }

  /**
   * Expose the given {@link View} as a Crunch {@link Target}.
   *
   * @param view the view to write to
   * @param <E>  the type of entity stored in the view
   * @return a {@link Target} for the view
   *
   * @since 0.14.0
   */
  public static <E> Target asTarget(View<E> view) {
    return new DatasetTarget<E>(view);
  }

  /**
   * Expose the {@link Dataset} or {@link View} represented by the given
   * URI as a Crunch {@link Target}.
   *
   * @param uri the dataset or view URI
   * @return a {@link Target} for the dataset or view
   *
   * @since 0.15.0
   */
  public static Target asTarget(String uri) {
    return asTarget(URI.create(uri));
  }

  /**
   * Expose the {@link Dataset} or {@link View} represented by the given
   * URI as a Crunch {@link Target}.
   *
   * @param uri the dataset or view URI
   * @return a {@link Target} for the dataset or view
   *
   * @since 0.15.0
   */
  public static Target asTarget(URI uri) {
    return new DatasetTarget<Object>(uri);
  }

  /**
   * Partitions {@code collection} to be stored efficiently in {@code View}.
   * <p>
   * This restructures the parallel collection so that all of the entities that
   * will be stored in a given partition will be processed by the same writer.
   *
   * @param collection a collection of entities
   * @param view a {@link View} of a dataset to partition the collection for
   * @param <E> the type of entities in the collection and underlying dataset
   * @return an equivalent collection of entities partitioned for the view
   *
   * @since 0.16.0
   */
  public static <E> PCollection<E> partition(PCollection<E> collection,
                                             View<E> view) {
    return partition(collection, view.getDataset(), -1);
  }

  /**
   * Partitions {@code collection} to be stored efficiently in {@code dataset}.
   * <p>
   * This restructures the parallel collection so that all of the entities that
   * will be stored in a given partition will be processed by the same writer.
   *
   * @param collection a collection of entities
   * @param dataset a dataset to partition the collection for
   * @param <E> the type of entities in the collection and underlying dataset
   * @return an equivalent collection of entities partitioned for the view
   *
   * @since 0.16.0
   */
  public static <E> PCollection<E> partition(PCollection<E> collection,
                                             Dataset<E> dataset) {
    return partition(collection, dataset, -1);
  }

  /**
   * Partitions {@code collection} to be stored efficiently in {@code View}.
   * <p>
   * This restructures the parallel collection so that all of the entities that
   * will be stored in a given partition will be processed by the same writer.
   * <p>
   * If the dataset is not partitioned, then this will structure all of the
   * entities to produce a number of files equal to {@code numWriters}.
   *
   * @param collection a collection of entities
   * @param view a {@link View} of a dataset to partition the collection for
   * @param numWriters the number of writers that should be used
   * @param <E> the type of entities in the collection and underlying dataset
   * @return an equivalent collection of entities partitioned for the view
   * @see #partition(PCollection, View)
   *
   * @since 0.16.0
   */
  public static <E> PCollection<E> partition(PCollection<E> collection,
                                             View<E> view,
                                             int numWriters) {
    return partition(collection, view, numWriters, 1);
  }

  /**
   * Partitions {@code collection} to be stored efficiently in {@code View}.
   * <p>
   * This restructures the parallel collection so that all of the entities that
   * will be stored in a given partition will be evenly distributed across a specified
   * {@code numPartitionWriters}.
   * <p>
   * If the dataset is not partitioned, then this will structure all of the
   * entities to produce a number of files equal to {@code numWriters}.
   *
   * @param collection a collection of entities
   * @param view a {@link View} of a dataset to partition the collection for
   * @param numWriters the number of writers that should be used
   * @param numPartitionWriters the number of writers data for a single partition will be distributed across
   * @param <E> the type of entities in the collection and underlying dataset
   * @return an equivalent collection of entities partitioned for the view
   * @see #partition(PCollection, View)
   *
   * @since 1.1.0
   */
  public static <E> PCollection<E> partition(PCollection<E> collection,
                                             View<E> view,
                                             int numWriters, int numPartitionWriters) {
    //ensure the number of writers is honored whether it is per partition or total.
    DatasetDescriptor descriptor = view.getDataset().getDescriptor();
    if (descriptor.isPartitioned()) {
      GetStorageKey<E> getKey = new GetStorageKey<E>(view, numPartitionWriters);
      PTable<Pair<GenericData.Record, Integer>, E> table = collection
          .by(getKey, Avros.pairs(Avros.generics(getKey.schema()), Avros.ints()));
      PGroupedTable<Pair<GenericData.Record, Integer>, E> grouped =
          numWriters > 0 ? table.groupByKey(numWriters) : table.groupByKey();
      return grouped.ungroup().values();
    } else {
      return partition(collection, numWriters);
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
      value={"SE_NO_SERIALVERSIONID","SE_TRANSIENT_FIELD_NOT_RESTORED"},
      justification="Purposely not supported across versions, fields properly initialized")
  private static class GetStorageKey<E> extends MapFn<E, Pair<GenericData.Record, Integer>> {
    private final String strategyString;
    private final String schemaString;
    private final Class<E> type;
    private final Map<String, String> constraints;
    private final int numPartitionWriters;
    private transient AvroStorageKey key = null;
    private transient EntityAccessor<E> accessor = null;
    private transient Map<String, Object> provided = null;
    private transient int count;

    private GetStorageKey(View<E> view, int numPartitionWriters) {
      DatasetDescriptor descriptor = view.getDataset().getDescriptor();
      // get serializable versions of transient objects
      this.strategyString = descriptor.getPartitionStrategy()
          .toString(false /* no white space */);
      this.schemaString = descriptor.getSchema()
          .toString(false /* no white space */);
      this.type = view.getType();
      if (view instanceof AbstractRefinableView) {
        this.constraints = ((AbstractRefinableView) view).getConstraints()
            .toQueryMap();
      } else {
        this.constraints = null;
      }
      this.numPartitionWriters = numPartitionWriters > 0 ? numPartitionWriters : 1;
    }

    public Schema schema() {
      initialize(); // make sure the key is not null
      return key.getSchema();
    }

    @Override
    public void initialize() {
      if (key == null) {
        // restore transient objects from serializable versions
        PartitionStrategy strategy = PartitionStrategyParser.parse(strategyString);
        Schema schema = new Schema.Parser().parse(schemaString);
        this.key = new AvroStorageKey(strategy, schema);
        this.accessor = DataModelUtil.accessor(type, schema);
        if (constraints != null) {
          this.provided = Constraints
              .fromQueryMap(schema, strategy, constraints)
              .getProvidedValues();
        }
      }
      count = 0;
    }

    @Override
    public Pair<GenericData.Record, Integer> map(E entity) {
      int marker = count % numPartitionWriters;
      count += 1;
      return Pair.<GenericData.Record, Integer>of(key.reuseFor(entity, provided, accessor), marker);
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="EQ_DOESNT_OVERRIDE_EQUALS",
      justification="StorageKey equals is correct, compares the values")
  private static class AvroStorageKey extends GenericData.Record {
    private final StorageKey key;

    private AvroStorageKey(PartitionStrategy strategy, Schema schema) {
      super(SchemaUtil.keySchema(schema, strategy));
      this.key = new StorageKey(strategy);
    }

    public <E> AvroStorageKey reuseFor(E entity,
                                       @Nullable Map<String, Object> provided,
                                       EntityAccessor<E> accessor) {
      key.reuseFor(entity, provided, accessor);
      return this;
    }

    @Override
    public void put(int i, Object v) {
      key.replace(i, v);
    }

    @Override
    public Object get(int i) {
      return key.get(i);
    }
  }

}
