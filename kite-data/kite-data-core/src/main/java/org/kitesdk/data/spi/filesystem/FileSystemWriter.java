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

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.DatasetWriterException;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.spi.ReaderWriterState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FileSystemWriter<E> implements DatasetWriter<E> {

  private static final Logger LOG = LoggerFactory.getLogger(FileSystemWriter.class);

  static interface FileAppender<E> extends Flushable, Closeable {
    public void open() throws IOException;
    public void append(E entity) throws IOException;
  }

  protected final FileSystem fs;
  private final Path directory;
  private final DatasetDescriptor descriptor;
  private FileAppender<E> appender;
  private Path tempPath;
  private Path finalPath;
  private ReaderWriterState state;
  private int count = 0;

  public FileSystemWriter(FileSystem fs, Path path, DatasetDescriptor descriptor) {
    Preconditions.checkNotNull(fs, "File system is not defined");
    Preconditions.checkNotNull(path, "Destination directory is not defined");
    Preconditions.checkNotNull(descriptor, "Descriptor is not defined");
    this.fs = fs;
    this.directory = path;
    this.descriptor = descriptor;
    this.state = ReaderWriterState.NEW;
  }

  @Override
  public final void open() {
    Preconditions.checkState(state.equals(ReaderWriterState.NEW),
        "Unable to open a writer from state:%s", state);

    // ensure the directory exists
    try {
      fs.mkdirs(directory);
    } catch (IOException ex) {
      this.state = ReaderWriterState.ERROR;
      throw new DatasetIOException("Failed to create path " + directory, ex);
    }

    // initialize paths
    this.finalPath = new Path(directory, uniqueFilename(descriptor.getFormat()));
    this.tempPath = tempFilename(finalPath);
    this.appender = newAppender(tempPath);

    try {
      appender.open();
    } catch (IOException e) {
      this.state = ReaderWriterState.ERROR;
      throw new DatasetIOException("Failed to open appender " + appender, e);
    }

    this.count = 0;

    LOG.debug("Opened appender {} for {}", appender, finalPath);

    this.state = ReaderWriterState.OPEN;
  }

  @Override
  public final void write(E entity) {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to write to a writer in state:%s", state);

    try {
      appender.append(entity);
      count += 1;
    } catch (IOException e) {
      this.state = ReaderWriterState.ERROR;
      throw new DatasetIOException(
          "Failed to append " + entity + " to " + appender, e);
    }
  }

  @Override
  public void flush() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to write to a writer in state:%s", state);
    try {
      appender.flush();
    } catch (IOException e) {
      this.state = ReaderWriterState.ERROR;
      throw new DatasetWriterException("Failed to flush appender " + appender);
    }
  }

  @Override
  public final void close() {
    if (state.equals(ReaderWriterState.OPEN)) {
      try {
        appender.close();
      } catch (IOException e) {
        this.state = ReaderWriterState.ERROR;
        throw new DatasetIOException("Failed to close appender " + appender, e);
      }

      if (count > 0) {
        // commit the temp file
        try {
          if (!fs.rename(tempPath, finalPath)) {
            this.state = ReaderWriterState.ERROR;
            throw new DatasetWriterException(
                "Failed to move " + tempPath + " to " + finalPath);
          }
        } catch (IOException e) {
          this.state = ReaderWriterState.ERROR;
          throw new DatasetIOException("Failed to commit " + finalPath, e);
        }

        LOG.debug("Committed {} for appender {}", finalPath, appender);
      } else {
        // discard the temp file
        try {
          if (!fs.delete(tempPath, true)) {
            this.state = ReaderWriterState.ERROR;
            throw new DatasetWriterException("Failed to delete " + tempPath);
          }
        } catch (IOException e) {
          this.state = ReaderWriterState.ERROR;
          throw new DatasetIOException(
              "Failed to remove temporary file " + tempPath, e);
        }

        LOG.debug("Discarded {} ({} entities)", tempPath, count);
      }
      this.state = ReaderWriterState.CLOSED;

    } else if (state.equals(ReaderWriterState.ERROR)) {
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

  @SuppressWarnings("unchecked")
  private <E> FileAppender<E> newAppender(Path temp) {
    Format format = descriptor.getFormat();
    if (Formats.PARQUET.equals(format)) {
      return (FileAppender<E>) new ParquetAppender(
          fs, temp, descriptor.getSchema(), true);
    } else if (Formats.AVRO.equals(format)) {
      return new AvroAppender<E>(fs, temp, descriptor.getSchema(), true);
    } else {
      this.state = ReaderWriterState.ERROR;
      throw new DatasetWriterException("Unknown format " + format);
    }
  }
}
