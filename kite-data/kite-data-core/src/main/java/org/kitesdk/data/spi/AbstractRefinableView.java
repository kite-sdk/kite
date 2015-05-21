/*
 * Copyright 2013 Cloudera.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spi;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import java.net.URI;
import java.util.Map;
import javax.annotation.concurrent.Immutable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.IncompatibleSchemaException;
import org.kitesdk.data.PartitionView;
import org.kitesdk.data.RefinableView;
import org.kitesdk.data.URIBuilder;
import org.kitesdk.data.View;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A common View base class to simplify implementations of Views created from ranges.
 *
 * @param <E>
 *      The type of entities stored in the {@code Dataset} underlying this
 *      {@code View}.
 * @since 0.9.0
 */
@Immutable
public abstract class AbstractRefinableView<E> implements RefinableView<E> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractRefinableView.class);

  protected final Dataset<E> dataset;
  protected final MarkerComparator comparator;
  protected final Constraints constraints;
  protected final EntityAccessor<E> accessor;
  protected final Predicate<E> entityTest;
  protected final boolean canRead;
  protected final boolean canWrite;

  // This class is Immutable and must be thread-safe
  protected final ThreadLocal<StorageKey> keys;

  protected AbstractRefinableView(Dataset<E> dataset, Class<E> type) {
    this.dataset = dataset;
    final DatasetDescriptor descriptor = dataset.getDescriptor();
    if (descriptor.isPartitioned()) {
      this.constraints = new Constraints(
          descriptor.getSchema(), descriptor.getPartitionStrategy());
      // TODO: is comparator used anywhere?
      this.comparator = new MarkerComparator(descriptor.getPartitionStrategy());
      this.keys = new ThreadLocal<StorageKey>() {
        @Override
        protected StorageKey initialValue() {
          return new StorageKey(descriptor.getPartitionStrategy());
        }
      };
    } else {
      this.constraints = new Constraints(descriptor.getSchema());
      this.comparator = null;
      this.keys = null;
    }
    this.accessor = DataModelUtil.accessor(type, descriptor.getSchema());
    this.entityTest = constraints.toEntityPredicate(accessor);

    Schema datasetSchema = descriptor.getSchema();
    this.canRead = SchemaValidationUtil.canRead(
        datasetSchema, accessor.getReadSchema());
    this.canWrite = SchemaValidationUtil.canRead(
        accessor.getWriteSchema(), datasetSchema);

    IncompatibleSchemaException.check(canRead || canWrite,
        "The type cannot be used to read from or write to the dataset:\n" +
        "Type schema: %s\nDataset schema: %s",
        getSchema(), descriptor.getSchema());
  }

  protected AbstractRefinableView(AbstractRefinableView<?> view, Schema schema, Class<E> type) {
    if (view.dataset instanceof AbstractDataset) {
      this.dataset = ((AbstractDataset<?>) view.dataset).asType(type);
    } else {
      this.dataset = Datasets.load(view.dataset.getUri(), type);
    }
    this.comparator = view.comparator;
    this.constraints = view.constraints;
    // thread-safe, so okay to reuse when views share a partition strategy
    this.keys = view.keys;
    // Resolve our type according to the given schema
    this.accessor = DataModelUtil.accessor(type, schema);
    this.entityTest = constraints.toEntityPredicate(accessor);

    Schema datasetSchema = dataset.getDescriptor().getSchema();
    this.canRead = SchemaValidationUtil.canRead(
        datasetSchema, accessor.getReadSchema());
    this.canWrite = SchemaValidationUtil.canRead(
        accessor.getWriteSchema(), datasetSchema);

    IncompatibleSchemaException.check(canRead || canWrite,
        "The type cannot be used to read from or write to the dataset:\n" +
        "Type schema: %s\nDataset schema: %s",
        getSchema(), datasetSchema);
  }

  protected AbstractRefinableView(AbstractRefinableView<E> view, Constraints constraints) {
    this.dataset = view.dataset;
    this.comparator = view.comparator;
    this.constraints = constraints;
    // thread-safe, so okay to reuse when views share a partition strategy
    this.keys = view.keys;
    // No need to resolve type here as it would have been resolved by our parent
    // view
    this.accessor = view.accessor;
    this.entityTest = constraints.toEntityPredicate(accessor);
    this.canRead = view.canRead;
    this.canWrite = view.canWrite;
  }

  public Constraints getConstraints() {
    return constraints;
  }

  protected abstract AbstractRefinableView<E> filter(Constraints c);

  protected abstract <T> AbstractRefinableView<T> project(Schema schema, Class<T> type);

  @Override
  public Dataset<E> getDataset() {
    return dataset;
  }

  @Override
  public boolean deleteAll() {
    throw new UnsupportedOperationException(
        "This Dataset does not support bulk deletion");
  }

  @Override
  public Class<E> getType() {
    return accessor.getType();
  }

  @Override
  public Schema getSchema() {
    return accessor.getReadSchema();
  }

  public EntityAccessor<E> getAccessor() {
    return accessor;
  }

  public Map<String, Object> getProvidedValues() {
    return constraints.getProvidedValues();
  }

  @Override
  public Iterable<PartitionView<E>> getCoveringPartitions() {
    throw new UnsupportedOperationException("This Dataset does not support " +
        "getCoveringPartitions.");
  }

  @Override
  public boolean includes(E entity) {
    return entityTest.apply(entity);
  }

  @Override
  public AbstractRefinableView<E> with(String name, Object... values) {
    return filter(constraints.with(name, values));
  }

  @Override
  public AbstractRefinableView<E> from(String name, Comparable value) {
    return filter(constraints.from(name, value));
  }

  @Override
  public AbstractRefinableView<E> fromAfter(String name, Comparable value) {
    return filter(constraints.fromAfter(name, value));
  }

  @Override
  public AbstractRefinableView<E> to(String name, Comparable value) {
    return filter(constraints.to(name, value));
  }

  @Override
  public AbstractRefinableView<E> toBefore(String name, Comparable value) {
    return filter(constraints.toBefore(name, value));
  }

  @Override
  @SuppressWarnings("unchecked")
  public AbstractRefinableView<GenericRecord> asSchema(Schema schema) {
    return project(schema, GenericRecord.class);
  }

  @Override
  public <T> View<T> asType(Class<T> type) {
    if (DataModelUtil.isGeneric(type)) {
      // if the type is generic, don't reset the schema
      return project(getSchema(), type);
    }
    // otherwise, the type determines the schema
    return project(getDataset().getDescriptor().getSchema(), type);
  }

  @Override
  public boolean isEmpty() {
    DatasetReader<E> reader = null;
    try {
      // use a reader because files may be present but empty
      reader = newReader();
      return !reader.hasNext();
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if ((o == null) || !Objects.equal(this.getClass(), o.getClass())) {
      return false;
    }

    AbstractRefinableView that = (AbstractRefinableView) o;
    return (Objects.equal(this.dataset, that.dataset) &&
        Objects.equal(this.constraints, that.constraints));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getClass(), dataset, constraints);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("dataset", dataset)
        .add("constraints", constraints)
        .toString();
  }

  @Override
  public URI getUri() {
    URIBuilder builder = new URIBuilder(dataset.getUri());
    for (Map.Entry<String, String> entry : constraints.toQueryMap().entrySet()) {
      builder.with(entry.getKey(), entry.getValue());
    }
    return builder.build();
  }

  protected Predicate<StorageKey> getKeyPredicate() {
    return constraints.toKeyPredicate();
  }

  protected void checkSchemaForWrite() {
    IncompatibleSchemaException.check(canWrite,
        "Cannot write data with this view's schema, " +
        "it cannot be read with the dataset's schema:\n" +
        "Current schema: %s\nDataset schema: %s",
        getSchema(), dataset.getDescriptor().getSchema());
  }

  protected void checkSchemaForRead() {
    IncompatibleSchemaException.check(canRead,
        "Cannot read data with this view's schema:\n" +
        "Current schema: %s\nDataset schema: %s",
        dataset.getDescriptor().getSchema(), getSchema());
  }
}
