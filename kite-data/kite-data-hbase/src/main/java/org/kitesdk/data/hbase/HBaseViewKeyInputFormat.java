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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.kitesdk.compat.Hadoop;
import org.kitesdk.data.hbase.impl.BaseDao;
import org.kitesdk.data.hbase.impl.BaseEntityScanner;
import org.kitesdk.data.hbase.impl.Dao;
import org.kitesdk.data.hbase.impl.EntityMapper;
import org.kitesdk.data.spi.AbstractKeyRecordReaderWrapper;
import org.kitesdk.data.spi.FilteredRecordReader;

import static org.apache.hadoop.hbase.mapreduce.TableInputFormat.SCAN;

class HBaseViewKeyInputFormat<E> extends InputFormat<E, Void> {

  private DaoDataset<E> dataset;
  private DaoView<E> view;
  private EntityMapper<E> entityMapper;

  public HBaseViewKeyInputFormat(DaoDataset<E> dataset) {
    this.dataset = dataset;
    this.view = null;
    Dao<E> dao = dataset.getDao();
    if (!(dao instanceof BaseDao)) {
      throw new UnsupportedOperationException("Only BaseDao subclasses supported.");
    }
    this.entityMapper = ((BaseDao<E>) dao).getEntityMapper();
  }

  public HBaseViewKeyInputFormat(DaoView<E> view) {
    this((DaoDataset<E>) view.getDataset());
    this.view = view;
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
    Configuration conf = Hadoop.JobContext.getConfiguration.invoke(jobContext);
    return getDelegate(conf).getSplits(jobContext);
  }

  @Override
  public RecordReader<E, Void> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    Configuration conf = Hadoop.TaskAttemptContext
        .getConfiguration.invoke(taskAttemptContext);
    TableInputFormat delegate = getDelegate(conf);
    RecordReader<E, Void> unfilteredRecordReader = new HBaseRecordReaderWrapper<E>(
        delegate.createRecordReader(inputSplit, taskAttemptContext), entityMapper);
    if (view != null) {
      // use the constraints to filter out entities from the reader
      return new FilteredRecordReader<E>(unfilteredRecordReader,
          view.getConstraints(), view.getAccessor());
    }
    return unfilteredRecordReader;
  }

  private TableInputFormat getDelegate(Configuration conf) throws IOException {
    TableInputFormat delegate = new TableInputFormat();
    String tableName = HBaseMetadataProvider.getTableName(dataset.getName());
    conf.set(TableInputFormat.INPUT_TABLE, tableName);
    if (view != null) {
      Job tempJob = new Job();
      Scan scan = ((BaseEntityScanner) view.newEntityScanner()).getScan();
      TableMapReduceUtil.initTableMapperJob(tableName, scan, TableMapper.class, null,
          null, tempJob);
      Configuration tempConf = Hadoop.JobContext.getConfiguration.invoke(tempJob);
      conf.set(SCAN, tempConf.get(SCAN));
    }
    delegate.setConf(conf);
    return delegate;
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
