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
package org.kitesdk.data.hbase.impl;

import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.spi.AbstractDatasetReader;
import org.kitesdk.data.spi.ReaderWriterState;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;

/**
 * Base EntityScanner implementation. This EntityScanner will use an
 * EntityMapper while scanning rows in HBase, and will map each row to an entity
 * 
 * @param <E>
 *          The entity type this scanner scans.
 */
public class BaseEntityScanner<E> extends AbstractDatasetReader<E>
    implements EntityScanner<E> {

  private final EntityMapper<E> entityMapper;
  private final HTablePool tablePool;
  private final String tableName;
  private Scan scan;
  private ResultScanner resultScanner;
  private Iterator<Result> iterator;
  private ReaderWriterState state;

  /**
   * @param scan
   *          The Scan object that will be used
   * @param tablePool
   *          The HTablePool instance to get a table to open a scanner on.
   * @param tableName
   *          The table name to perform the scan on.
   * @param entityMapper
   *          The EntityMapper to map rows to entities..
   */
  public BaseEntityScanner(Scan scan, HTablePool tablePool, String tableName,
      EntityMapper<E> entityMapper) {
    this.scan = scan;
    this.entityMapper = entityMapper;
    this.tablePool = tablePool;
    this.tableName = tableName;

    this.state = ReaderWriterState.NEW;
  }

  /**
   * Construct a BaseEntityScanner using a Builder
   * 
   * @param scanBuilder
   *          Build object to construct the BaseEntityScanner with
   */
  private BaseEntityScanner(Builder<E> scanBuilder) {
    this.entityMapper = scanBuilder.getEntityMapper();
    this.tablePool = scanBuilder.getTablePool();
    this.tableName = scanBuilder.getTableName();
    this.scan = new Scan();

    if (scanBuilder.getStartKey() != null) {
      byte[] keyBytes = entityMapper.getKeySerDe().serialize(
          scanBuilder.getStartKey());
      if (!scanBuilder.getStartInclusive()) {
        keyBytes = addZeroByte(keyBytes);
      }
      this.scan.setStartRow(keyBytes);
    }

    if (scanBuilder.getStopKey() != null) {
      byte[] keyBytes = entityMapper.getKeySerDe().serialize(
          scanBuilder.getStopKey());
      if (scanBuilder.getStopInclusive()) {
        keyBytes = addZeroByte(keyBytes);
      }
      this.scan.setStopRow(keyBytes);
    }

    if (scanBuilder.getCaching() != 0) {
      this.scan.setCaching(scanBuilder.getCaching());
    }

    if (scanBuilder.getEntityMapper() != null) {
      HBaseUtils.addColumnsToScan(entityMapper.getRequiredColumns(), this.scan);
    }

    // If Filter List Was Built, Add It To The Scanner
    if (scanBuilder.getFilterList().size() > 0) {
      // Check if this is a PASS_ALL or PASS_ONE List
      if (scanBuilder.getPassAllFilters()) {
        FilterList filterList = new FilterList(
            FilterList.Operator.MUST_PASS_ALL, scanBuilder.getFilterList());
        this.scan.setFilter(filterList);
      } else {
        FilterList filterList = new FilterList(
            FilterList.Operator.MUST_PASS_ONE, scanBuilder.getFilterList());
        this.scan.setFilter(filterList);
      }
    }

    for (ScanModifier scanModifier : scanBuilder.getScanModifiers()) {
      this.scan = scanModifier.modifyScan(this.scan);
    }

    this.state = ReaderWriterState.NEW;
  }
  
  @Override
  public Iterator<E> iterator() {
    return this;
  }

  @Override
  public void initialize() {
    Preconditions.checkState(state.equals(ReaderWriterState.NEW),
        "A scanner may not be opened more than once - current state:%s", state);

    HTableInterface table = null;
    try {
      table = tablePool.getTable(tableName);
      try {
        resultScanner = table.getScanner(scan);
      } catch (IOException e) {
        throw new DatasetIOException("Failed to fetch scanner", e);
      }
    } finally {
      if (table != null) {
        try {
          table.close();
        } catch (IOException e) {
          throw new DatasetIOException("Error putting table back into pool",
              e);
        }
      }
    }
    iterator = resultScanner.iterator();

    state = ReaderWriterState.OPEN;
  }

  @Override
  public void close() {
    if (!state.equals(ReaderWriterState.OPEN)) {
      return;
    }

    resultScanner.close();

    state = ReaderWriterState.CLOSED;
  }

  @Override
  public boolean isOpen() {
    return state.equals(ReaderWriterState.OPEN);
  }

  @Override
  public boolean hasNext() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to read from a scanner in state:%s", state);

    return iterator.hasNext();
  }

  public E read() {
    throw new UnsupportedOperationException("read() not supported, use next().");
  }

  @Override
  public E next() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to read from a scanner in state:%s", state);

    Result result = iterator.next();
    if (result == null) {
      throw new NoSuchElementException();
    }
    return entityMapper.mapToEntity(result);
  }

  @Override
  public void remove() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to read from a scanner in state:%s", state);

    iterator.remove();
  }

  public Scan getScan() {
    return scan;
  }

  /**
   * Scanner builder for BaseEntityScanner
   * 
   * @param <K>
   * @param <E>
   */
  public static class Builder<E> extends EntityScannerBuilder<E> {

    public Builder(HTablePool tablePool, String tableName,
        EntityMapper<E> entityMapper) {
      super(tablePool, tableName, entityMapper);
    }

    @Override
    public BaseEntityScanner<E> build() {
      return new BaseEntityScanner<E>(this);
    }

  }
  
  private byte[] addZeroByte(byte[] inBytes) {
    byte[] outBytes = new byte[inBytes.length + 1];
    System.arraycopy(inBytes, 0, outBytes, 0, inBytes.length);
    outBytes[inBytes.length] = (byte)0;
    return outBytes;
  }
}
