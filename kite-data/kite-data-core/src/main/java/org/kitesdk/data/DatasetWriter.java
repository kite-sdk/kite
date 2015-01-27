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
package org.kitesdk.data;

import java.io.Closeable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * <p>
 * A stream-oriented dataset writer.
 * </p>
 * <p>
 * Implementations of this interface write data to a {@link Dataset}.
 * Writers are use-once objects that serialize entities of type {@code E} and
 * write them to the underlying storage system. Normally, you are
 * not expected to instantiate implementations directly. Instead, use the
 * containing dataset's {@link Dataset#newWriter()} method to get an appropriate
 * implementation. You should receive an instance of this interface from a
 * dataset, invoke {@link #write(Object)} and {@link #close()} when they are done,
 * or no more data exists.
 * </p>
 * <p>
 * Implementations can hold system resources until the {@link #close()} method
 * is called, so you <strong>must</strong> follow the normal try / finally
 * pattern to ensure these resources are properly freed when the writer is no
 * longer needed. Do not rely on implementations automatically invoking the
 * {@code close} method upon object finalization (implementations must not do
 * so). All implementations must silently ignore multiple invocations of
 * {@code close} as well as a close of an unopened writer.
 * </p>
 * <p>
 * If any method throws an exception other than {@link DatasetRecordException},
 * the writer is no longer valid, and the only method that can be subsequently
 * called is {@code close}.
 * </p>
 * <p>
 * Implementations of {@link DatasetWriter} are typically not thread-safe; that
 * is, the behavior when accessing a single instance from multiple threads is
 * undefined.
 * </p>
 *
 * @param <E> The type of entity accepted by this writer.
 */
@NotThreadSafe
public interface DatasetWriter<E> extends Closeable {

  /**
   * <p>
   * Write an entity to the underlying dataset.
   * </p>
   * <p>
   * If any exception other than {@link DatasetRecordException} is thrown, this
   * writer is no longer valid and should be closed.
   * </p>
   *
   * @param entity The entity to write
   * @throws DatasetRecordException
   *            If a record could not be written, but the writer is still valid.
   * @throws DatasetIOException
   *            To wrap an internal {@link java.io.IOException}
   */
  void write(E entity);

  /**
   * <p>
   * Close the writer and release any system resources. If this method returns without
   * throwing an exception then any entity that was successfully written with
   * {@link #write(Object)} will be stored to stable storage.
   * </p>
   * <p>
   * No further operations of this interface (other than additional calls to
   * this method) can be performed; however, implementations can choose to
   * permit other method calls. See implementation documentation for details.
   * </p>
   * @throws DatasetOperationException
   *            If the operation did not succeed.
   * @throws DatasetIOException
   *            To wrap an internal {@link java.io.IOException}

   */
  @Override
  void close();

  boolean isOpen();

}
