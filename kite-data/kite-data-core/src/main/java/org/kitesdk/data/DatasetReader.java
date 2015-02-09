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
import java.util.Iterator;
import java.util.NoSuchElementException;
import javax.annotation.concurrent.NotThreadSafe;
import edu.umd.cs.findbugs.annotations.SuppressWarnings;

/**
 * <p>
 * A stream-oriented dataset reader.
 * </p>
 * <p>
 * Implementations of this interface read data from a {@link Dataset}.
 * Readers are use-once objects that read from the underlying storage system to
 * produce deserialized entities of type {@code E}. Normally, you do not
 * instantiate implementations directly. Instead, use the containing dataset's
 * {@link Dataset#newReader()} method to get an appropriate implementation.
 * You receive an instance of this interface from a dataset, invoke
 * {@link #hasNext()} and {@link #next()} as necessary, and {@link #close()}
 * when you are done or no more data exists.
 * </p>
 * <p>
 * Implementations can hold system resources until you call the {@link #close()}
 * method, so you <strong>must</strong> follow the normal try/finally
 * pattern to ensure these resources are properly freed when the reader is
 * exhausted or no longer useful. You should not rely on implementations
 * automatically invoking the {@code close()} method upon object finalization
 * (although you are free to do so, if you choose). Your implementations must
 * silently ignore multiple invocations of {@code close()}, as well as a close
 * of an unopened reader.
 * </p>
 * <p>
 * If any method throws an exception, the reader is no longer valid. The
 * only method that you can call after an exception is {@code close()}.
 * </p>
 * <p>
 * Implementations of {@link DatasetReader} are not required to be thread-safe;
 * that is, the behavior when accessing a single instance from multiple threads
 * is undefined.
 * </p>
 *
 * @param <E> The type of entity produced by this reader.
 */
@NotThreadSafe
public interface DatasetReader<E> extends Iterator<E>, Iterable<E>, Closeable {

  /**
   * Test the reader to see if additional entities can be read.
   *
   * @return true if additional entities exist, false otherwise.
   * @throws DatasetReaderException
   */
  @Override
  boolean hasNext();

  /**
   * <p>
   * Fetch the next entity from the reader.
   * </p>
   * <p>
   * Calling this method when no additional data exists is illegal; you should
   * use {@link #hasNext()} to test if a call to {@code read()} will succeed.
   * Implementations of this method can block.
   * </p>
   *
   * @return An entity of type {@code E}.
   * @throws DatasetReaderException
   * @throws NoSuchElementException
   *
   * @since 0.7.0
   */
  @SuppressWarnings(value="IT_NO_SUCH_ELEMENT",
      justification="Implementations should throw NoSuchElementException")
  @Override
  E next();

  /**
   * <p>
   * Removes from the underlying collection the last element returned by the
   * reader (OPTIONAL). This has the same semantics as {@link
   * Iterator#remove()}.
   * </p>
   * For most applications, you do not need to implement {@code remove}.
   * </p>
   *
   * @since 0.7.0
   */
  @Override
  void remove();

  /**
   * <p>
   * Close the reader and release any system resources.
   * </p>
   *
   * @throws DatasetReaderException
   */
  @Override
  void close();

  boolean isOpen();
}
