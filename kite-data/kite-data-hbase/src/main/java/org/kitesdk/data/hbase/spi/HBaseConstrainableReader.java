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
package org.kitesdk.data.hbase.spi;

import org.kitesdk.data.hbase.impl.EntityScanner;
import org.kitesdk.data.hbase.impl.EntityScannerBuilder;
import org.kitesdk.data.spi.InitializeAccessor;
import org.kitesdk.data.spi.ServerSideConstrainableReader;

import java.util.Iterator;
import java.util.Map;

/**
 * Implementation of {@link ServerSideConstrainableReader} for HBase.
 * Wraps a {@link EntityScannerBuilder} to apply server-side constraints
 * and then create the {@link EntityScanner} on initialize
 *
 * @param <E>
 */
public class HBaseConstrainableReader<E>
    implements ServerSideConstrainableReader<E>, InitializeAccessor {
  private EntityScannerBuilder<E> scannerBuilder;
  private EntityScanner<E> iterator;

  public HBaseConstrainableReader(EntityScannerBuilder<E> scannerBuilder) {
    this.scannerBuilder = scannerBuilder;
  }

  @Override
  public void applyFilters(
      Map<String, Object> serverSideFilters) {
    for (Map.Entry<String, Object> predicateEntry : serverSideFilters
        .entrySet()) {
      String fieldName = predicateEntry.getKey();
      Object value = predicateEntry.getValue();
      if (value != null) {
        scannerBuilder.addEqualFilter(fieldName, value);
      } else {
        scannerBuilder.addNotNullFilter(fieldName);
      }
    }
  }

  @Override
  public Iterator<E> iterator() {
    return iterator;
  }

  @Override
  public void initialize() {
    iterator = scannerBuilder.build();
    iterator.initialize();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public E next() {
    return iterator.next();
  }

  @Override
  public void remove() {
    iterator.remove();
  }

  @Override
  public void close() {
    iterator.close();
  }

  @Override
  public boolean isOpen() {
    return iterator != null && iterator.isOpen();
  }
}
