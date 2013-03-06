package com.cloudera.data;

import java.io.IOException;

/**
 * <p>
 * A stream-oriented dataset reader.
 * </p>
 * <p>
 * Subsystem-specific implementations of this interface are used to read data
 * from a {@link Dataset}. Readers are use-once objects that produce entities of
 * type {@code E}. Normally, users are not expected to instantiate
 * implementations directly. Instead, use the containing dataset's
 * {@link Dataset#getReader()} method to get an appropriate implementation.
 * Normally, users receive an instance of this interface from a dataset, call
 * {@link #open()} to prepare for IO operations, invoke {@link #hasNext()} and
 * {@link #read()} as necessary, and {@link #close()} when they are done or no
 * more data exists.
 * </p>
 * <p>
 * Implementations may hold system resources until the {@link #close()} method
 * is called, so users <strong>must</strong> follow the normal try / finally
 * pattern to ensure these resources are properly freed when the reader is
 * exhausted or no longer useful. Do not rely on implementations automatically
 * invoking the {@code close()} method upon object finalization (although
 * implementations are free to do so, if they choose). All implementations must
 * silently ignore multiple invocations of {@code close()} as well as a close of
 * an unopened reader.
 * </p>
 * <p>
 * If any method throws an exception, the reader is no longer valid, and the
 * only method that may be subsequently called is {@code close()}.
 * </p>
 * 
 * @param <E>
 *          The type of entity produced by this reader.
 */
public interface DatasetReader<E> {

  /**
   * <p>
   * Open the reader, allocating any necessary resources required to produce
   * entities.
   * </p>
   * <p>
   * This method <strong>must</strong> be invoked prior to any calls of
   * {@link #hasNext()} or {@link #read()}.
   * </p>
   * 
   * @throws IOException
   */
  void open() throws IOException;

  /**
   * Tests the reader to see if additional entities can be read.
   * 
   * @return true if additional entities exist, false otherwise.
   * @throws IOException
   */
  boolean hasNext() throws IOException;

  /**
   * <p>
   * Fetch the next entity from the reader.
   * </p>
   * <p>
   * Calling this method when no additional data exists is illegal; users should
   * use {@link #hasNext()} to test if a call to {@code read()} will succeed.
   * Implementations of this method may block.
   * </p>
   * 
   * @return An entity of type {@code E}.
   * @throws IOException
   */
  E read() throws IOException;

  /**
   * <p>
   * Close the reader and release any system resources.
   * </p>
   * <p>
   * No further operations of this interface (other than additional calls of
   * this method) may be performed, however implementations may choose to permit
   * other method calls. See implementation documentation for details.
   * </p>
   * 
   * @throws IOException
   */
  void close() throws IOException;

}
