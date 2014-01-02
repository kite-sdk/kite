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
package org.kitesdk.data.filesystem;

import java.lang.reflect.Constructor;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.UnknownFormatException;
import org.kitesdk.data.spi.AbstractDatasetReader;
import org.kitesdk.data.spi.ReaderWriterState;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

class MultiFileDatasetReader<E> extends AbstractDatasetReader<E> {

  private final FileSystem fileSystem;
  private final DatasetDescriptor descriptor;

  private final Iterator<Path> filesIter;
  private DatasetReader<E> reader = null;

  private ReaderWriterState state;

  public MultiFileDatasetReader(FileSystem fileSystem, Iterable<Path> files,
      DatasetDescriptor descriptor) {
    Preconditions.checkArgument(fileSystem != null, "FileSystem cannot be null");
    Preconditions.checkArgument(descriptor != null, "Descriptor cannot be null");
    Preconditions.checkArgument(files != null, "Descriptor cannot be null");

    this.fileSystem = fileSystem;
    this.descriptor = descriptor;
    this.filesIter = files.iterator();
    this.state = ReaderWriterState.NEW;
  }

  @Override
  public void open() {
    Preconditions.checkState(state.equals(ReaderWriterState.NEW),
      "A reader may not be opened more than once - current state:%s", state);

    final Format format = descriptor.getFormat();
    if (!(Formats.AVRO.equals(format) || Formats.PARQUET.equals(format)
        || Formats.CSV.equals(format) || Formats.MORPHLINE.equals(format))) {
      throw new UnknownFormatException("Cannot open format:" + format.getName());
    }

    this.state = ReaderWriterState.OPEN;
  }

  @SuppressWarnings("unchecked") // See https://github.com/Parquet/parquet-mr/issues/106
  private void openNextReader() {
    if (Formats.PARQUET.equals(descriptor.getFormat())) {
      reader = new ParquetFileSystemDatasetReader(fileSystem, filesIter.next(),
          descriptor.getSchema());
    } else if (Formats.CSV.equals(descriptor.getFormat())) {
      reader = new CSVFileReader<E>(fileSystem, filesIter.next(), descriptor);
    } else if (Formats.MORPHLINE.equals(descriptor.getFormat())) {
      //reader = new MorphlineDatasetReader<E>(fileSystem, filesIter.next(), descriptor);
      // TODO: make the list of supported formats more dynamically pluggable 
      // and extensible to formats contained in external maven modules?
      try {
        Class clazz = Class.forName("org.kitesdk.data.morphline.MorphlineDatasetReader");
        Constructor ctor = clazz.getConstructor(FileSystem.class, Path.class, DatasetDescriptor.class);
        reader = (DatasetReader<E>) ctor.newInstance(fileSystem, filesIter.next(), descriptor);
      } catch (RuntimeException e) {
        throw new UnknownFormatException("Cannot open format:" + descriptor.getFormat().getName(), e);
      } catch (Exception e) {
        throw new UnknownFormatException("Cannot open format:" + descriptor.getFormat().getName(), e);
      }
    } else {
      reader = new FileSystemDatasetReader<E>(fileSystem, filesIter.next(),
          descriptor.getSchema());
    }
    reader.open();
  }

  @Override
  public boolean hasNext() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
      "Attempt to read from a file in state:%s", state);

    while (true) {
      if (reader == null) {
        if (filesIter.hasNext()) {
          openNextReader();
        } else {
          return false;
        }
      } else {
        if (reader.hasNext()) {
          return true;
        } else {
          reader.close();
          reader = null;
        }
      }
    }
  }

  @Override
  public E next() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
      "Attempt to read from a file in state:%s", state);
    if (hasNext()) {
      // if hasNext => true, then reader is guaranteed to be non-null
      return reader.next();
    } else {
      throw new NoSuchElementException();
    }
  }

  @Override
  public void remove() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to remove from a file in state:%s", state);
    if (reader == null) {
      // If next succeeds, then reader cannot be null. If the reader is null,
      // then next did not succeed or hasNext has been used and the reader is
      // gone. We could keep track of the last reader that next returned a value
      // from, but it probably isn't worth it.
      throw new IllegalStateException(
          "Remove can only be called after next() returns a value and before calling hasNext()");
    }
    reader.remove();
  }

  @Override
  public void close() {
    if (!state.equals(ReaderWriterState.OPEN)) {
      return;
    }
    if (reader != null) {
      reader.close();
    }
    state = ReaderWriterState.CLOSED;
  }

  @Override
  public boolean isOpen() {
    return state.equals(ReaderWriterState.OPEN);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("fileSystem", fileSystem)
      .add("descriptor", descriptor)
      .add("filesIter", filesIter)
      .add("reader", reader)
      .add("state", state)
      .toString();
  }

}
