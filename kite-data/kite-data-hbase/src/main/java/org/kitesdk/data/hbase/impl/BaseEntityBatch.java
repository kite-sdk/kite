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
import org.kitesdk.data.Flushable;
import org.kitesdk.data.spi.AbstractDatasetWriter;
import org.kitesdk.data.spi.ReaderWriterState;
import com.google.common.base.Preconditions;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

public class BaseEntityBatch<E> extends AbstractDatasetWriter<E>
    implements EntityBatch<E>, Flushable {
  private final HTableInterface table;
  private final EntityMapper<E> entityMapper;
  private final HBaseClientTemplate clientTemplate;
  private ReaderWriterState state;

  /**
   * Checks an HTable out of the HTablePool and modifies it to take advantage of
   * batch puts. This is very useful when performing many consecutive puts.
   *
   * @param clientTemplate
   *          The client template to use
   * @param entityMapper
   *          The EntityMapper to use for mapping
   * @param pool
   *          The HBase table pool
   * @param tableName
   *          The name of the HBase table
   * @param writeBufferSize
   *          The batch buffer size in bytes.
   */
  public BaseEntityBatch(HBaseClientTemplate clientTemplate,
      EntityMapper<E> entityMapper, HTablePool pool, String tableName,
      long writeBufferSize) {
    this.table = pool.getTable(tableName);
    this.table.setAutoFlush(false);
    this.clientTemplate = clientTemplate;
    this.entityMapper = entityMapper;
    this.state = ReaderWriterState.NEW;

    /**
     * If the writeBufferSize is less than the currentBufferSize, then the
     * buffer will get flushed automatically by HBase. This should never happen,
     * since we're getting a fresh table out of the pool, and the writeBuffer
     * should be empty.
     */
    try {
      table.setWriteBufferSize(writeBufferSize);
    } catch (IOException e) {
      throw new DatasetIOException("Error flushing commits for table ["
          + table + "]", e);
    }
  }

  /**
   * Checks an HTable out of the HTablePool and modifies it to take advantage of
   * batch puts using the default writeBufferSize (2MB). This is very useful
   * when performing many consecutive puts.
   *
   * @param clientTemplate
   *          The client template to use
   * @param entityMapper
   *          The EntityMapper to use for mapping
   * @param pool
   *          The HBase table pool
   * @param tableName
   *          The name of the HBase table
   */
  public BaseEntityBatch(HBaseClientTemplate clientTemplate,
      EntityMapper<E> entityMapper, HTablePool pool, String tableName) {
    this.table = pool.getTable(tableName);
    this.table.setAutoFlush(false);
    this.clientTemplate = clientTemplate;
    this.entityMapper = entityMapper;
    this.state = ReaderWriterState.NEW;
  }

  @Override
  public void initialize() {
    Preconditions.checkState(state.equals(ReaderWriterState.NEW),
        "Unable to open a writer from state:%s", state);
    state = ReaderWriterState.OPEN;
  }

  @Override
  public void put(E entity) {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to write to a writer in state:%s", state);

    PutAction putAction = entityMapper.mapFromEntity(entity);
    clientTemplate.put(putAction, table);
  }

  @Override
  public void write(E entity) {
    put(entity);
  }

  @Override
  public void flush() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to flush a writer in state:%s", state);

    try {
      table.flushCommits();
    } catch (IOException e) {
      throw new DatasetIOException("Error flushing commits for table ["
          + table + "]", e);
    }
  }

  @Override
  public void close() {
    if (state.equals(ReaderWriterState.OPEN)) {
      try {
        table.flushCommits();
        table.setAutoFlush(true);
        table.close();
      } catch (IOException e) {
        throw new DatasetIOException("Error closing table [" + table + "]", e);
      }
      state = ReaderWriterState.CLOSED;
    }
  }

  @Override
  public boolean isOpen() {
    return state.equals(ReaderWriterState.OPEN);
  }
}
