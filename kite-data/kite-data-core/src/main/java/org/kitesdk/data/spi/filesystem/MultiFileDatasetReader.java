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
package org.kitesdk.data.spi.filesystem;

import com.google.common.collect.Iterators;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.UnknownFormatException;
import org.kitesdk.data.spi.AbstractDatasetReader;
import org.kitesdk.data.spi.Constraints;
import org.kitesdk.data.spi.EntityAccessor;
import org.kitesdk.data.spi.ReaderWriterState;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileSystem;

import java.util.Iterator;
import java.util.NoSuchElementException;

class MultiFileDatasetReader<E> extends AbstractDatasetReader<E> {

  private final FileSystem fileSystem;
  private final DatasetDescriptor descriptor;
  private final Constraints constraints;
  private final EntityAccessor<E> accessor;

  private final Iterator<Path> filesIter;
  private final PathIterator pathIter;
  private AbstractDatasetReader<E> reader = null;
  private Iterator<E> readerIterator = null;

  private ReaderWriterState state;

  public MultiFileDatasetReader(FileSystem fileSystem, Iterable<Path> files,
      DatasetDescriptor descriptor, Constraints constraints,
      EntityAccessor<E> accessor) {
    Preconditions.checkNotNull(fileSystem, "FileSystem cannot be null");
    Preconditions.checkNotNull(descriptor, "Descriptor cannot be null");
    Preconditions.checkNotNull(files, "Partition paths cannot be null");

    this.fileSystem = fileSystem;
    this.descriptor = descriptor;
    this.constraints = constraints;
    this.filesIter = files.iterator();
    this.state = ReaderWriterState.NEW;
    if (files instanceof PathIterator) {
      this.pathIter = (PathIterator) files;
    } else {
      this.pathIter = null;
    }
    this.accessor = accessor;
  }

  @Override
  public void initialize() {
    Preconditions.checkState(state.equals(ReaderWriterState.NEW),
      "A reader may not be opened more than once - current state:%s", state);

    final Format format = descriptor.getFormat();
    if (!(Formats.AVRO.equals(format) || Formats.PARQUET.equals(format)
        || Formats.CSV.equals(format))) {
      throw new UnknownFormatException("Cannot open format:" + format.getName());
    }

    this.state = ReaderWriterState.OPEN;
  }

  @SuppressWarnings("unchecked") // See https://github.com/Parquet/parquet-mr/issues/106
  private void openNextReader() {
    if (Formats.PARQUET.equals(descriptor.getFormat())) {
      this.reader = new ParquetFileSystemDatasetReader(fileSystem,
          filesIter.next(), accessor.getEntitySchema(), accessor.getType());
    } else if (Formats.CSV.equals(descriptor.getFormat())) {
      this.reader = new CSVFileReader<E>(fileSystem, filesIter.next(),
          descriptor, accessor);
    } else {
      this.reader = new FileSystemDatasetReader<E>(fileSystem, filesIter.next(),
          accessor.getEntitySchema(), accessor.getType());
    }
    reader.initialize();
    this.readerIterator = Iterators.filter(reader,
        constraints.toEntityPredicate(
            (pathIter != null ? pathIter.getStorageKey() : null), accessor));
  }

  @Override
  public boolean hasNext() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
      "Attempt to read from a file in state:%s", state);

    while (true) {
      if (readerIterator == null) {
        if (filesIter.hasNext()) {
          openNextReader();
        } else {
          return false;
        }
      } else {
        if (readerIterator.hasNext()) {
          return true;
        } else {
          readerIterator = null;
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
      return readerIterator.next();
    } else {
      throw new NoSuchElementException();
    }
  }

  @Override
  public void remove() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to remove from a file in state:%s", state);
    if (readerIterator == null) {
      // If next succeeds, then reader cannot be null. If the reader is null,
      // then next did not succeed or hasNext has been used and the reader is
      // gone. We could keep track of the last reader that next returned a value
      // from, but it probably isn't worth it.
      throw new IllegalStateException(
          "Remove can only be called after next() returns a value and before calling hasNext()");
    }
    readerIterator.remove();
  }

  @Override
  public void close() {
    if (!state.equals(ReaderWriterState.OPEN)) {
      return;
    }
    if (reader != null) {
      reader.close();
      reader = null;
      readerIterator = null;
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
