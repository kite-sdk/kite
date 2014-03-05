/**
 * Copyright 2014 Cloudera Inc.
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

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.hbase.impl.BaseDao;
import org.kitesdk.data.hbase.impl.Dao;
import org.kitesdk.data.hbase.impl.EntityMapper;
import org.kitesdk.data.spi.AbstractKeyRecordReaderWrapper;

class HBaseDatasetKeyInputFormat<E> extends InputFormat<E, Void> {

  private Dataset<E> dataset;
  private EntityMapper<E> entityMapper;

  public HBaseDatasetKeyInputFormat(DaoDataset<E> dataset) {
    this.dataset = dataset;
    Dao<E> dao = dataset.getDao();
    if (!(dao instanceof BaseDao)) {
      throw new UnsupportedOperationException("Only BaseDao subclasses supported.");
    }
    this.entityMapper = ((BaseDao<E>) dao).getEntityMapper();
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException,
      InterruptedException {
    TableInputFormat delegate = new TableInputFormat();
    String tableName = HBaseMetadataProvider.getTableName(dataset.getName());
    jobContext.getConfiguration().set(TableInputFormat.INPUT_TABLE, tableName);
    delegate.setConf(jobContext.getConfiguration());
    return delegate.getSplits(jobContext);
  }

  @Override
  public RecordReader<E, Void> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    TableInputFormat delegate = new TableInputFormat();
    String tableName = HBaseMetadataProvider.getTableName(dataset.getName());
    taskAttemptContext.getConfiguration().set(TableInputFormat.INPUT_TABLE, tableName);
    delegate.setConf(taskAttemptContext.getConfiguration());
    return new HBaseRecordReaderWrapper<E>(
        delegate.createRecordReader(inputSplit, taskAttemptContext), entityMapper);
  }

  private static class HBaseRecordReaderWrapper<E> extends
      AbstractKeyRecordReaderWrapper<E, ImmutableBytesWritable, Result> {
    private final EntityMapper<E> entityMapper;

    public HBaseRecordReaderWrapper(RecordReader<ImmutableBytesWritable, Result> delegate,
        EntityMapper<E> entityMapper) {
      super(delegate);
      this.entityMapper = entityMapper;
    }

    public E getCurrentKey() throws IOException, InterruptedException {
      return entityMapper.mapToEntity(delegate.getCurrentValue());
    }
  }
}
