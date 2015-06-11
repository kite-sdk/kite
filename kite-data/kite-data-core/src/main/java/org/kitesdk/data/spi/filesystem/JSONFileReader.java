/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spi.filesystem;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.spi.AbstractDatasetReader;
import org.kitesdk.data.spi.DataModelUtil;
import org.kitesdk.data.spi.EntityAccessor;
import org.kitesdk.data.spi.JsonUtil;
import org.kitesdk.data.spi.ReaderWriterState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JSONFileReader<E> extends AbstractDatasetReader<E> {

  private static final Logger LOG = LoggerFactory
      .getLogger(JSONFileReader.class);

  private final FileSystem fs;
  private final Path path;
  private final GenericData model;
  private final Schema schema;

  // progress reporting
  private long size = 0;
  private InputStream incoming = null;

  // state
  private ReaderWriterState state = ReaderWriterState.NEW;
  private Iterator<E> iterator;

  public JSONFileReader(FileSystem fileSystem, Path path,
                        EntityAccessor<E> accessor) {
    this.fs = fileSystem;
    this.path = path;
    this.schema = accessor.getReadSchema();
    this.model = DataModelUtil.getDataModelForType(accessor.getType());
    this.state = ReaderWriterState.NEW;
  }

  public JSONFileReader(InputStream incoming, Schema schema, Class<E> type) {
    this.fs = null;
    this.path = null;
    this.incoming = incoming;
    this.schema = schema;
    this.model = DataModelUtil.getDataModelForType(type);
    this.state = ReaderWriterState.NEW;
  }

  @Override
  public void initialize() {
    Preconditions.checkState(state.equals(ReaderWriterState.NEW),
        "A reader may not be opened more than once - current state:%s", state);
    Preconditions.checkArgument(Schema.Type.RECORD.equals(schema.getType()),
        "Schemas for JSON files should be record");

    if (incoming == null) {
      Preconditions.checkNotNull(fs, "FileSystem cannot be null");
      Preconditions.checkNotNull(path, "Path cannot be null");
      try {
        this.incoming = fs.open(path);
        this.size = fs.getFileStatus(path).getLen();
      } catch (IOException ex) {
        throw new DatasetIOException("Cannot open path: " + path, ex);
      }
    }

    this.iterator = Iterators.transform(JsonUtil.parser(incoming),
        new Function<JsonNode, E>() {
          @Override
          @SuppressWarnings("unchecked")
          public E apply(@Nullable JsonNode node) {
            return (E) JsonUtil.convertToAvro(model, node, schema);
          }
        });

    this.state = ReaderWriterState.OPEN;
  }

  @Override
  public boolean hasNext() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to read from a file in state:%s", state);
    return iterator.hasNext();
  }

  @Override
  public E next() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to read from a file in state:%s", state);
    return iterator.next();
  }

  @Override
  public void close() {
    if (!state.equals(ReaderWriterState.OPEN)) {
      return;
    }

    LOG.debug("Closing reader on path:{}", path);

    iterator = null;
    try {
      incoming.close();
    } catch (IOException e) {
      throw new DatasetIOException("Unable to close reader path:" + path, e);
    }

    state = ReaderWriterState.CLOSED;
  }

  @Override
  public boolean isOpen() {
    return (this.state == ReaderWriterState.OPEN);
  }

  public RecordReader<E, Void> asRecordReader() {
    Preconditions.checkArgument(incoming instanceof FSDataInputStream,
        "Cannot use {} in a record reader", incoming.getClass());
    return new JSONRecordReader();
  }

  public class JSONRecordReader extends RecordReader<E, Void> {
    private E current;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (hasNext()) {
        this.current = next();
        return true;
      }
      return false;
    }

    @Override
    public E getCurrentKey() throws IOException, InterruptedException {
      return current;
    }

    @Override
    public Void getCurrentValue() throws IOException, InterruptedException {
      return null;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      if (size == 0) {
        return 0.0f;
      }
      return ((float) ((FSDataInputStream) incoming).getPos()) / size;
    }

    @Override
    public void close() throws IOException {
      JSONFileReader.this.close();
    }
  }
}
