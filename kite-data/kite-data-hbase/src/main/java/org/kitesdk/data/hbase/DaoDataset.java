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

import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Key;
import org.kitesdk.data.PartitionKey;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.RandomAccessDataset;
import org.kitesdk.data.RefineableView;
import org.kitesdk.data.hbase.impl.Dao;
import org.kitesdk.data.spi.AbstractDataset;

// TODO: this should not be public!
public class DaoDataset<E> extends AbstractDataset<E> implements RandomAccessDataset<E> {

  private String name;
  private Dao<E> dao;
  private DatasetDescriptor descriptor;
  private final DaoView<E> unbounded;

  public DaoDataset(String name, Dao<E> dao, DatasetDescriptor descriptor) {
    this.name = name;
    this.dao = dao;
    this.descriptor = descriptor;
    this.unbounded = new DaoView<E>(this);
  }

  public Dao<E> getDao() {
    return dao;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public DatasetDescriptor getDescriptor() {
    return descriptor;
  }

  @Override
  public Dataset<E> getPartition(PartitionKey key, boolean autoCreate) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropPartition(PartitionKey key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterable<Dataset<E>> getPartitions() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected RefineableView<E> asRefineableView() {
    return unbounded;
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

    return strategy.partitionKey(values);
  }
}
