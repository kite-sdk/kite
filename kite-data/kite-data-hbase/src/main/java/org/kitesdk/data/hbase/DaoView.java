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
package org.kitesdk.data.hbase;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.InputFormat;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Flushable;
import org.kitesdk.data.hbase.impl.EntityBatch;
import org.kitesdk.data.hbase.impl.EntityScanner;
import org.kitesdk.data.impl.Accessor;
import org.kitesdk.data.spi.AbstractDatasetReader;
import org.kitesdk.data.spi.AbstractDatasetWriter;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.PartitionKey;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.spi.AbstractRefinableView;
import org.kitesdk.data.spi.Constraints;
import org.kitesdk.data.spi.InitializeAccessor;
import org.kitesdk.data.spi.InputFormatAccessor;
import org.kitesdk.data.spi.StorageKey;
import org.kitesdk.data.spi.Marker;
import org.kitesdk.data.spi.MarkerRange;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;

class DaoView<E> extends AbstractRefinableView<E> implements InputFormatAccessor<E> {

  private final DaoDataset<E> dataset;

  DaoView(DaoDataset<E> dataset, Class<E> type) {
    super(dataset, type);
    this.dataset = dataset;
  }

  private DaoView(DaoView<E> view, Constraints constraints) {
    super(view, constraints);
    this.dataset = view.dataset;
  }

  private DaoView(DaoView<?> view, Schema schema, Class<E> type) {
    super(view, schema, type);
    this.dataset = (DaoDataset<E>) view.dataset.asType(type);
  }

  @Override
  protected DaoView<E> filter(Constraints constraints) {
    return new DaoView<E>(this, constraints);
  }

  @Override
  protected <T> AbstractRefinableView<T> project(Schema schema, Class<T> type) {
    return new DaoView<T>(this, schema, type);
  }

  EntityScanner<E> newEntityScanner() {
    Iterable<MarkerRange> markerRanges = constraints.toKeyRanges();
    // TODO: combine all ranges into a single reader
    MarkerRange range = Iterables.getOnlyElement(markerRanges);
    return dataset.getDao().getScanner(
        toPartitionKey(range.getStart()), range.getStart().isInclusive(),
        toPartitionKey(range.getEnd()), range.getEnd().isInclusive());
  }

  @Override
  public DatasetReader<E> newReader() {
    final DatasetReader<E> wrappedReader = newEntityScanner();
    final Iterator<E> filteredIterator = constraints.filter(
        wrappedReader.iterator(), getAccessor());
    AbstractDatasetReader<E> reader = new AbstractDatasetReader<E>() {
      @Override
      public void initialize() {
        if (wrappedReader instanceof InitializeAccessor) {
          ((InitializeAccessor) wrappedReader).initialize();
        }
      }

      @Override
      public boolean hasNext() {
        Preconditions.checkState(isOpen(),
            "Attempt to read from a scanner that is not open");
        return filteredIterator.hasNext();
      }

      @Override
      public E next() {
        Preconditions.checkState(isOpen(),
            "Attempt to read from a scanner that is not open");
        return filteredIterator.next();
      }

      @Override
      public void remove() {
        Preconditions.checkState(isOpen(),
            "Attempt to read from a scanner that is not open");
        filteredIterator.remove();
      }

      @Override
      public void close() {
        wrappedReader.close();
      }

      @Override
      public boolean isOpen() {
        return wrappedReader.isOpen();
      }

      @Override
      public Iterator<E> iterator() {
        return filteredIterator;
      }
    };
    reader.initialize();
    return reader;
  }

  @Override
  public DatasetWriter<E> newWriter() {
    final EntityBatch<E> wrappedWriter = dataset.getDao().newBatch();
    if (constraints.isUnbounded()) {
      wrappedWriter.initialize();
      return wrappedWriter;
    }
    final StorageKey partitionStratKey = new StorageKey(dataset.getDescriptor().getPartitionStrategy());
    // Return a dataset writer that checks on write that an entity is within the
    // range of the view
    AbstractDatasetWriter<E> writer = new AbstractFlushableDatasetWriter<E>() {
      private Constraints.KeyPredicate keyPredicate = constraints.toKeyPredicate();

      @Override
      public void initialize() {
        wrappedWriter.initialize();
      }

      @Override
      public void write(E entity) {
        StorageKey key = getAccessor().keyFor(entity,
            constraints.getProvidedValues(), partitionStratKey);
        if (!keyPredicate.apply(key)) {
          throw new IllegalArgumentException("View does not contain entity: " + entity);
        }
        wrappedWriter.write(entity);
      }

      @Override
      public void flush() {
        wrappedWriter.flush();
      }

      @Override
      public void close() {
        wrappedWriter.close();
      }

      @Override
      public boolean isOpen() {
        return wrappedWriter.isOpen();
      }
    };
    writer.initialize();
    return writer;
  }

  abstract static class AbstractFlushableDatasetWriter<E> extends AbstractDatasetWriter<E>
      implements Flushable {
  }

  @SuppressWarnings("deprecation")
  PartitionKey toPartitionKey(MarkerRange.Boundary boundary) {
    if (boundary == null || boundary.getBound() == null) {
      return null;
    }

    return keyFor(dataset.getDescriptor().getPartitionStrategy(),
        boundary.getBound());
  }

  @Deprecated
  @SuppressWarnings("unchecked")
  static PartitionKey keyFor(PartitionStrategy strategy, Marker marker) {
    final List<FieldPartitioner> partitioners =
        Accessor.getDefault().getFieldPartitioners(strategy);
    final Object[] values = new Object[partitioners.size()];

    for (int i = 0, n = partitioners.size(); i < n; i += 1) {
      final FieldPartitioner fp = partitioners.get(i);
      values[i] = marker.valueFor(fp);
    }

    return new PartitionKey(values);
  }

  @Override
  public InputFormat<E, Void> getInputFormat(Configuration conf) {
    return new HBaseViewKeyInputFormat<E>(this);
  }
}
