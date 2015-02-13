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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import java.io.Closeable;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.DatasetOperationException;
import org.kitesdk.data.DatasetRecordException;
import org.kitesdk.data.Flushable;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.Syncable;
import org.kitesdk.data.UnknownFormatException;
import org.kitesdk.data.ValidationException;
import org.kitesdk.data.spi.AbstractDatasetWriter;
import org.kitesdk.data.spi.DescriptorUtil;
import org.kitesdk.data.spi.ReaderWriterState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FileSystemWriter<E> extends AbstractDatasetWriter<E> {

  private static final Logger LOG = LoggerFactory.getLogger(FileSystemWriter.class);
  private static final Set<Format> SUPPORTED_FORMATS = ImmutableSet
      .<Format>builder()
      .add(Formats.AVRO)
      .add(Formats.PARQUET)
      .build();

  static boolean isSupportedFormat(DatasetDescriptor descriptor) {
    Format format = descriptor.getFormat();
    return (SUPPORTED_FORMATS.contains(format) || (Formats.CSV.equals(format) &&
        DescriptorUtil.isEnabled(FileSystemProperties.ALLOW_CSV_PROP, descriptor)
    ));
  }

  static interface FileAppender<E> extends java.io.Flushable, Closeable {
    public void open() throws IOException;
    public void append(E entity) throws IOException;
    public void sync() throws IOException;
    public void cleanup() throws IOException;
  }

  private final Path directory;
  private final DatasetDescriptor descriptor;
  private Path tempPath;
  private Path finalPath;
  private int count = 0;

  protected final FileSystem fs;
  protected FileAppender<E> appender;
  protected boolean flushed = false;

  @VisibleForTesting
  ReaderWriterState state;

  @VisibleForTesting
  final Configuration conf;

  private FileSystemWriter(FileSystem fs, Path path, DatasetDescriptor descriptor) {
    Preconditions.checkNotNull(fs, "File system is not defined");
    Preconditions.checkNotNull(path, "Destination directory is not defined");
    Preconditions.checkNotNull(descriptor, "Descriptor is not defined");
    this.fs = fs;
    this.directory = path;
    this.descriptor = descriptor;
    this.conf = new Configuration(fs.getConf());
    this.state = ReaderWriterState.NEW;

    // copy file format settings from custom properties to the Configuration
    for (String prop : descriptor.listProperties()) {
      conf.set(prop, descriptor.getProperty(prop));
    }
  }

  @Override
  public final void initialize() {
    Preconditions.checkState(state.equals(ReaderWriterState.NEW),
        "Unable to open a writer from state:%s", state);

    ValidationException.check(isSupportedFormat(descriptor),
        "Not a supported format: %s", descriptor.getFormat());

    // ensure the directory exists
    try {
      fs.mkdirs(directory);
    } catch (RuntimeException e) {
      this.state = ReaderWriterState.ERROR;
      throw new DatasetOperationException(e,
          "Failed to create path %s", directory);
    } catch (IOException ex) {
      this.state = ReaderWriterState.ERROR;
      throw new DatasetIOException("Failed to create path " + directory, ex);
    }

    // initialize paths
    try {
      this.finalPath = new Path(directory, uniqueFilename(descriptor.getFormat()));
      this.tempPath = tempFilename(finalPath);
    } catch (RuntimeException e) {
      this.state = ReaderWriterState.ERROR;
      throw new DatasetOperationException(e,
          "Failed to initialize file paths under %s", directory);
    }

    try {
      this.appender = newAppender(tempPath);
      appender.open();
    } catch (RuntimeException e) {
      this.state = ReaderWriterState.ERROR;
      throw new DatasetOperationException(e,
          "Failed to open appender %s", appender);
    } catch (IOException e) {
      this.state = ReaderWriterState.ERROR;
      throw new DatasetIOException("Failed to open appender " + appender, e);
    }

    this.count = 0;

    LOG.info("Opened output appender {} for {}", appender, finalPath);

    this.state = ReaderWriterState.OPEN;
  }

  @Override
  public final void write(E entity) {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to write to a writer in state:%s", state);

    try {
      appender.append(entity);
      count += 1;
    } catch (RuntimeException e) {
      Throwables.propagateIfInstanceOf(e, DatasetRecordException.class);
      this.state = ReaderWriterState.ERROR;
      throw new DatasetOperationException(e,
          "Failed to append %s to %s", entity, appender);
    } catch (IOException e) {
      this.state = ReaderWriterState.ERROR;
      throw new DatasetIOException(
          "Failed to append " + entity + " to " + appender, e);
    }
  }

  @Override
  public final void close() {
    try {
      if (ReaderWriterState.NEW.equals(state) ||
          ReaderWriterState.CLOSED.equals(state)) {
        return;
      }

      // Only try to close the appender if not in an error state. Any calls to
      // flush and sync must produce recoverable data without a call to close.
      if (!ReaderWriterState.ERROR.equals(state)) {
        try {
          appender.close();
        } catch (RuntimeException e) {
          throw new DatasetOperationException(e,
              "Failed to close appender %s", appender);
        } catch (IOException e) {
          throw new DatasetIOException("Failed to close appender " + appender, e);
        }
      }

      // Make the file visible if any data was written and either some data has
      // been flushed or the writer is not in an error state. Only instances of
      // IncrementalWriter set flushed to true.
      if (count > 0 && (flushed || ReaderWriterState.OPEN.equals(state))) {
        // commit the temp file
        try {
          if (!fs.rename(tempPath, finalPath)) {
            throw new DatasetOperationException(
                "Failed to move %s to %s", tempPath, finalPath);
          }
        } catch (RuntimeException e) {
          throw new DatasetOperationException(e,
              "Failed to commit %s", finalPath);
        } catch (IOException e) {
          throw new DatasetIOException("Failed to commit " + finalPath, e);
        }

        LOG.debug("Committed {} for appender {} ({} entities)",
            new Object[]{finalPath, appender, count});

      } else {
        // discard the temp file
        try {
          if (!fs.delete(tempPath, true)) {
            throw new DatasetOperationException(
                "Failed to delete %s", tempPath);
          }
        } catch (RuntimeException e) {
          throw new DatasetOperationException(e,
              "Failed to remove temporary file %s", tempPath);
        } catch (IOException e) {
          throw new DatasetIOException(
              "Failed to remove temporary file " + tempPath, e);
        }

        LOG.debug("Discarded {} ({} entities)", tempPath, count);
      }

      try {
        appender.cleanup();
      } catch (IOException e) {
        throw new DatasetIOException("Failed to clean up " + appender, e);
      }

    } finally {
      this.state = ReaderWriterState.CLOSED;
    }
  }

  @Override
  public final boolean isOpen() {
    return state.equals(ReaderWriterState.OPEN);
  }

  private static String uniqueFilename(Format format) {
    return UUID.randomUUID() + "." + format.getExtension();
  }

  private static Path tempFilename(Path location) {
    return new Path(location.getParent(), "." + location.getName() + ".tmp");
  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  <E> FileAppender<E> newAppender(Path temp) {
    Format format = descriptor.getFormat();
    if (Formats.PARQUET.equals(format)) {
      // by default, Parquet is not durable
      if (DescriptorUtil.isDisabled(
          FileSystemProperties.NON_DURABLE_PARQUET_PROP, descriptor)) {
        return (FileAppender<E>) new DurableParquetAppender(
            fs, temp, descriptor.getSchema(), conf, descriptor.getCompressionType());
      } else {
        return (FileAppender<E>) new ParquetAppender(
            fs, temp, descriptor.getSchema(), conf,
            descriptor.getCompressionType());
      }
    } else if (Formats.AVRO.equals(format)) {
      return new AvroAppender<E>(fs, temp, descriptor.getSchema(),
          descriptor.getCompressionType());
    } else if (Formats.CSV.equals(format) &&
        DescriptorUtil.isEnabled(FileSystemProperties.ALLOW_CSV_PROP, descriptor)) {
      return new CSVAppender<E>(fs, temp, descriptor);
    } else {
      this.state = ReaderWriterState.ERROR;
      throw new UnknownFormatException("Unknown format " + descriptor);
    }
  }

  static <E> FileSystemWriter<E> newWriter(FileSystem fs, Path path,
                                           DatasetDescriptor descriptor) {
    Format format = descriptor.getFormat();
    if (Formats.PARQUET.equals(format)) {
      // by default, Parquet is not durable
      if (DescriptorUtil.isDisabled(
          FileSystemProperties.NON_DURABLE_PARQUET_PROP, descriptor)) {
        return new IncrementalWriter<E>(fs, path, descriptor);
      } else {
        return new FileSystemWriter<E>(fs, path, descriptor);
      }
    } else if (Formats.AVRO.equals(format) || Formats.CSV.equals(format)) {
      return new IncrementalWriter<E>(fs, path, descriptor);
    } else {
      return new FileSystemWriter<E>(fs, path, descriptor);
    }
  }

  static class IncrementalWriter<E> extends FileSystemWriter<E>
      implements Flushable, Syncable {
    private IncrementalWriter(FileSystem fs, Path path,
                              DatasetDescriptor descriptor) {
      super(fs, path, descriptor);
    }

    @Override
    public void flush() {
      Preconditions.checkState(isOpen(),
          "Attempt to flush a writer in state:%s", state);
      try {
        appender.flush();
        this.flushed = true;
      } catch (RuntimeException e) {
        this.state = ReaderWriterState.ERROR;
        throw new DatasetOperationException(e,
            "Failed to flush appender %s", appender);
      } catch (IOException e) {
        this.state = ReaderWriterState.ERROR;
        throw new DatasetIOException("Failed to flush appender " + appender, e);
      }
    }

    @Override
    public void sync() {
      Preconditions.checkState(isOpen(),
          "Attempt to sync a writer in state:%s", state);
      try {
        appender.sync();
        this.flushed = true;
      } catch (RuntimeException e) {
        this.state = ReaderWriterState.ERROR;
        throw new DatasetOperationException(e,
            "Failed to sync appender %s", appender);
      } catch (IOException e) {
        this.state = ReaderWriterState.ERROR;
        throw new DatasetIOException("Failed to sync appender " + appender, e);
      }
    }
  }

  public RecordWriter<E, Void> asRecordWriter() {
    return new KiteRecordWriter();
  }

  private class KiteRecordWriter extends RecordWriter<E, Void> {
    @Override
    public void write(E e, Void aVoid) throws IOException, InterruptedException {
      FileSystemWriter.this.write(e);
    }

    @Override
    public void close(TaskAttemptContext context)
        throws IOException, InterruptedException {
      FileSystemWriter.this.close();
    }
  }
}
