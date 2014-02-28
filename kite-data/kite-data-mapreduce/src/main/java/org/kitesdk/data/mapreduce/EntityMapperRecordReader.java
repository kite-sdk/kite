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
package org.kitesdk.data.mapreduce;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.kitesdk.data.hbase.impl.EntityMapper;

class EntityMapperRecordReader<E> extends RecordReader<E, Void> {

  private final RecordReader<ImmutableBytesWritable, Result> tableRecordReader;
  private final EntityMapper<E> entityMapper;
  private E entity;

  public EntityMapperRecordReader(
      RecordReader<ImmutableBytesWritable, Result> recordReader,
      EntityMapper<E> entityMapper) {
    this.tableRecordReader = recordReader;
    this.entityMapper = entityMapper;
  }

  @Override
  public void close() throws IOException {
    tableRecordReader.close();
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    tableRecordReader.initialize(split, context);
  }

  public E getCurrentKey() throws IOException, InterruptedException {
    return entity;
  }

  public Void getCurrentValue() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    boolean ret = tableRecordReader.nextKeyValue();
    if (ret) {
      Result result = tableRecordReader.getCurrentValue();
      entity = entityMapper.mapToEntity(result);
    }
    return ret;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return tableRecordReader.getProgress();
  }
}
