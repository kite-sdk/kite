package com.cloudera.data;

import java.io.IOException;

/**
 * <p>
 * A stream-oriented dataset writer.
 * </p>
 * <p>
 * Subsystem-specific implementations of this interface are used to write data
 * to a {@link Dataset}. Writers are use-once objects that serialize entities of
 * type {@code E} and write them to the underlying system. Normally, users are
 * not expected to instantiate implementations directly. Instead, use the
 * containing dataset's {@link Dataset#getWriter()} method to get an appropriate
 * implementation. Users should receive an instance of this interface from a
 * dataset, call {@link #open()} to prepare for IO operations, invoke
 * {@link #write(Object))} and {@link #flush()} as necessary, and
 * {@link #close()} when they are done, or no more data exists.
 * </p>
 * <p>
 * Implementations may hold system resources until the {@link #close()} method
 * is called, so users <strong>must</strong> follow the normal try / finally
 * pattern to ensure these resources are properly freed when the writer is no
 * longer needed. Do not rely on implementations automatically invoking the
 * {@code close()} method upon object finalization (implementations must not do
 * so). All implementations must silently ignore multiple invocations of
 * {@code close()} as well as a close of an unopened writer.
 * </p>
 * <p>
 * If any method throws an exception, the writer is no longer valid, and the
 * only method that may be subsequently called is {@code close()}.
 * </p>
 * 
 * @param <E>
 *          The type of entity accepted by this writer.
 */
public interface DatasetWriter<E> {

  /**
   * <p>
   * Open the writer, allocating any necessary resources required to store
   * entities.
   * </p>
   * <p>
   * This method <strong>must</strong> be invoked prior to any calls of
   * {@link #write(Object))} or {@link #flush()}.
   * </p>
   * 
   * @throws IOException
   */
  void open() throws IOException;

  /**
   * <p>
   * Writer an entity of type {@code E} to the associated dataset.
   * </p>
   * <p>
   * Implementations may buffer entities internally (see the {@link #flush()}
   * method). All instances of {@code entity} must conform to the dataset's
   * schema. If they don't, implementations should throw an exception.
   * </p>
   * 
   * @param entity
   * @throws IOException
   */
  void write(E entity) throws IOException;

  /**
   * <p>
   * Force or commit any outstanding data to storage.
   * </p>
   * </p>
   * <p>
   * Implementations of this interface must declare their durability guarantees.
   * </p>
   * 
   * @throws IOException
   */
  void flush() throws IOException;

  /**
   * <p>
   * Close the writer and release any system resources.
   * </p>
   * <p>
   * No further operations of this interface (other than additional calls to
   * this method) may be performed, however implementations may choose to permit
   * other method calls. See implementation documentation for details.
   * </p>
   * 
   * @throws IOException
   */
  void close() throws IOException;

}
