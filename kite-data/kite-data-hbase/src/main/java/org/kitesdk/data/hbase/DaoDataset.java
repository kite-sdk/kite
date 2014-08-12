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
import java.net.URI;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Key;
import org.kitesdk.data.spi.PartitionKey;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.RandomAccessDataset;
import org.kitesdk.data.RefinableView;
import org.kitesdk.data.hbase.impl.Dao;
import org.kitesdk.data.spi.AbstractDataset;
import org.kitesdk.data.spi.Constraints;
import org.kitesdk.data.spi.InputFormatAccessor;

class DaoDataset<E> extends AbstractDataset<E> implements RandomAccessDataset<E>,
    InputFormatAccessor<E> {

  private final String namespace;
  private final String name;
  private final Dao<E> dao;
  private final DatasetDescriptor descriptor;
  private final URI uri;
  private final DaoView<E> unbounded;

  public DaoDataset(String namespace, String name, Dao<E> dao, DatasetDescriptor descriptor,
      URI uri, Class<E> type) {
    super(type, descriptor.getSchema());
    Preconditions.checkArgument(IndexedRecord.class.isAssignableFrom(type) ||
            type == Object.class,
        "HBase only supports the generic and specific data models. The entity"
            + " type must implement IndexedRecord");
    this.namespace = namespace;
    this.name = name;
    this.dao = dao;
    this.descriptor = descriptor;
    this.uri = uri;
    this.unbounded = new DaoView<E>(this, type);
  }

  Dao<E> getDao() {
    return dao;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getNamespace() {
    return namespace;
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public DatasetDescriptor getDescriptor() {
    return descriptor;
  }

  @Override
  protected RefinableView<E> asRefinableView() {
    return unbounded;
  }

  @Override
  public boolean isEmpty() {
    return unbounded.isEmpty();
  }

  @Override
  public DaoView<E> filter(Constraints c) {
    return unbounded.filter(c);
  }

  @Override
  @SuppressWarnings("deprecation")
  public E get(Key key) {
    return dao.get(keyFor(getDescriptor().getPartitionStrategy(), key));
  }

  @Override
  public boolean put(E entity) {
    return dao.put(entity);
  }

  @Override
  @SuppressWarnings("deprecation")
  public long increment(Key key, String fieldName, long amount) {
    return dao.increment(keyFor(getDescriptor().getPartitionStrategy(), key), fieldName, amount);
  }

  @Override
  @SuppressWarnings("deprecation")
  public void delete(Key key) {
    dao.delete(keyFor(getDescriptor().getPartitionStrategy(), key));
  }

  @Override
  public boolean delete(E entity) {
    return dao.delete(entity);
  }

  @Deprecated
  static PartitionKey keyFor(PartitionStrategy strategy, Key key) {
    final int size = strategy.getFieldPartitioners().size();
    final Object[] values = new Object[size];

    for (int i = 0; i < size; i += 1) {
      values[i] = key.get(i);
    }

    return new PartitionKey(values);
  }

  @Override
  public InputFormat<E, Void> getInputFormat(Configuration conf) {
    return new HBaseViewKeyInputFormat<E>(this);
  }
}
