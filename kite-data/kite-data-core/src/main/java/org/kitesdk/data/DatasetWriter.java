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
import java.io.Flushable;
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
 * dataset, invoke {@link #write(Object)} and {@link #flush()} as necessary,
 * and {@link #close()} when they are done, or no more data exists.
 * </p>
 * <p>
 * Implementations can hold system resources until the {@link #close()} method
 * is called, so you <strong>must</strong> follow the normal try / finally
 * pattern to ensure these resources are properly freed when the writer is no
 * longer needed. Do not rely on implementations automatically invoking the
 * {@code close()} method upon object finalization (implementations must not do
 * so). All implementations must silently ignore multiple invocations of
 * {@code close()} as well as a close of an unopened writer.
 * </p>
 * <p>
 * If any method throws an exception, the writer is no longer valid, and the
 * only method that can be subsequently called is {@code close()}.
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
public interface DatasetWriter<E> extends Flushable, Closeable {

  /**
   * <p>
   * Write an entity of type {@code E} to the associated dataset.
   * </p>
   * <p>
   * Implementations can buffer entities internally (see the {@link #flush()}
   * method). All instances of {@code entity} must conform to the dataset's
   * schema. If they don't, implementations should throw an exception, although
   * this is not required.
   * </p>
   *
   * @param entity The entity to write
   * @throws DatasetWriterException
   */
  void write(E entity);

  /**
   * <p>
   * Force or commit any outstanding buffered data to the underlying stream.
   * </p>
   * <p>
   * Implementations of this interface must declare their durability guarantees.
   * </p>
   *
   * @throws DatasetWriterException
   */
  @Override
  void flush();

  /**
   * <p>
   * Ensure that data in the underlying stream has been written to disk.
   * </p>
   * <p>
   * Implementations of this interface must declare their durability guarantees.
   * </p>
   *
   * @throws DatasetWriterException
   */
  void sync();

  /**
   * <p>
   * Close the writer and release any system resources.
   * </p>
   * <p>
   * No further operations of this interface (other than additional calls to
   * this method) can be performed; however, implementations can choose to 
   * permit other method calls. See implementation documentation for details.
   * </p>
   *
   * @throws DatasetWriterException
   */
  @Override
  void close();

  boolean isOpen();

}
