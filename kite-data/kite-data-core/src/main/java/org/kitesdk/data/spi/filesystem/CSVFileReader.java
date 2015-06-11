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

import au.com.bytecode.opencsv.CSVReader;
import java.io.InputStream;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.spi.AbstractDatasetReader;
import org.kitesdk.data.spi.DescriptorUtil;
import org.kitesdk.data.spi.EntityAccessor;
import org.kitesdk.data.spi.ReaderWriterState;
import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.NoSuchElementException;

import static org.kitesdk.data.spi.filesystem.FileSystemProperties.REUSE_RECORDS;

public class CSVFileReader<E> extends AbstractDatasetReader<E> {

  private static final Logger LOG = LoggerFactory
      .getLogger(CSVFileReader.class);

  private final CSVProperties props;
  private final FileSystem fs;
  private final Path path;
  private final Schema schema;
  private final boolean reuseRecords;

  private final Class<E> recordClass;

  private CSVReader reader = null;
  private CSVRecordBuilder<E> builder;

  // progress reporting
  private long size = 0;
  private InputStream incoming = null;

  // state
  private ReaderWriterState state = ReaderWriterState.NEW;
  private boolean hasNext = false;
  private String[] next = null;
  private E record = null;

  @SuppressWarnings("unchecked")
  public CSVFileReader(FileSystem fileSystem, Path path,
                       DatasetDescriptor descriptor,
                       EntityAccessor<E> accessor) {
    this.fs = fileSystem;
    this.path = path;
    this.schema = accessor.getReadSchema();
    this.recordClass = accessor.getType();
    this.state = ReaderWriterState.NEW;
    this.props = CSVProperties.fromDescriptor(descriptor);
    // defaults to false: assume that callers will not make defensive copies
    this.reuseRecords = DescriptorUtil.isEnabled(REUSE_RECORDS, descriptor);

    Preconditions.checkArgument(Schema.Type.RECORD.equals(schema.getType()),
        "Schemas for CSV files must be records of primitive types");
  }

  public CSVFileReader(InputStream incoming, CSVProperties props,
                       Schema schema, Class<E> type) {
    this.fs = null;
    this.path = null;
    this.incoming = incoming;
    this.schema = schema;
    this.recordClass = type;
    this.state = ReaderWriterState.NEW;
    this.props = props;
    this.reuseRecords = false;

    Preconditions.checkArgument(Schema.Type.RECORD.equals(schema.getType()),
        "Schemas for CSV files must be records of primitive types");
  }

  @Override
  @SuppressWarnings("unchecked")
  public void initialize() {
    Preconditions.checkState(state.equals(ReaderWriterState.NEW),
        "A reader may not be opened more than once - current state:%s", state);

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

    this.reader = CSVUtil.newReader(incoming, props);

    List<String> header = null;
    if (props.useHeader) {
      this.hasNext = advance();
      header = Lists.newArrayList(next);
    } else if (props.header != null) {
      try {
        header = Lists.newArrayList(
            CSVUtil.newParser(props).parseLine(props.header));
      } catch (IOException e) {
        throw new DatasetIOException(
            "Failed to parse header from properties: " + props.header, e);
      }
    }

    this.builder = new CSVRecordBuilder<E>(schema, recordClass, header);

    // initialize by reading the first record
    this.hasNext = advance();

    this.state = ReaderWriterState.OPEN;
  }

  @Override
  public boolean hasNext() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to read from a file in state:%s", state);
    return hasNext;
  }

  @Override
  public E next() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to read from a file in state:%s", state);

    if (!hasNext) {
      throw new NoSuchElementException();
    }

    try {
      if (reuseRecords) {
        this.record = builder.makeRecord(next, record);
        return record;
      } else {
        return builder.makeRecord(next, null);
      }
    } finally {
      this.hasNext = advance();
    }
  }

  private boolean advance() {
    try {
      next = reader.readNext();
    } catch (IOException ex) {
      throw new DatasetIOException("Could not read record", ex);
    }
    return (next != null);
  }

  @Override
  public void close() {
    if (!state.equals(ReaderWriterState.OPEN)) {
      return;
    }

    LOG.debug("Closing reader on path:{}", path);

    try {
      reader.close();
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
    return new CSVRecordReader();
  }

  public class CSVRecordReader extends RecordReader<E, Void> {
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
      CSVFileReader.this.close();
    }
  }
}
