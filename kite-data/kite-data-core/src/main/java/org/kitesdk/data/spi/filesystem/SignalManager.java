/**
 * Copyright 2015 Cloudera Inc.
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.spi.Constraints;
import org.kitesdk.data.Signalable;

import com.google.common.base.Joiner;

/**
 * Manager for creating, and checking {@link Signalable#isReady() ready} signals.
 * Stored in a filesystem, typically HDFS.
 */
public class SignalManager {

  private final Path signalDirectory;
  private final FileSystem rootFileSystem;

  private static final String UNBOUNDED_CONSTRAINT = "unbounded";

  /**
   * Creates a new signal manager using the given signal directory.
   *
   * @param conf the Hadoop configuration
   * @param signalDirectory directory in which the manager
   *                        stores signals.
   *
   * @return a signal manager instance.
   */
  public SignalManager(FileSystem fileSystem, Path signalDirectory) {
    this.signalDirectory = signalDirectory;
    this.rootFileSystem = fileSystem;
  }

  /**
   * Create a signal for the specified constraints.
   *
   * @param viewConstraints The constraints to create a signal for.
   *
   * @throws DatasetException if the signal could not be created.
   */
  public void signalReady(Constraints viewConstraints) {
    try {
      rootFileSystem.mkdirs(signalDirectory);
    } catch (IOException e) {
      throw new DatasetIOException("Unable to create signal manager directory: "
              + signalDirectory, e);
    }

    String normalizedConstraints = getNormalizedConstraints(viewConstraints);

    Path signalPath = new Path(signalDirectory, normalizedConstraints);
    try{
      // create the output stream to overwrite the current contents, if the directory or file
      // exists it will be overwritten to get a new timestamp
      FSDataOutputStream os = rootFileSystem.create(signalPath, true);
      os.close();
    } catch (IOException e) {
      throw new DatasetIOException("Could not access signal path: " + signalPath, e);
    }
  }

  /**
   * Check the last time the specified constraints have been signaled as ready.
   *
   * @param viewConstraints The constraints to check for a signal.
   *
   * @return the timestamp of the last time the constraints were signaled as ready.
   *          if the constraints have never been signaled, -1 will be returned.
   *
   * @throws DatasetException if the signals could not be accessed.
   */
  public long getReadyTimestamp(Constraints viewConstraints) {
    String normalizedConstraints = getNormalizedConstraints(viewConstraints);

    Path signalPath = new Path(signalDirectory, normalizedConstraints);
    // check if the signal exists
    try {
      try {
        FileStatus signalStatus = rootFileSystem.getFileStatus(signalPath);
        return signalStatus.getModificationTime();
      } catch (final FileNotFoundException ex) {
        // empty, will be thrown when the signal path doesn't exist
      }
      return -1;
    } catch (IOException e) {
      throw new DatasetIOException("Could not access signal path: " + signalPath, e);
    }
  }

  /**
   * Get a normalized query string for the {@link Constraints} that identifies a
   * logical {@code View}.
   *
   * The normalized constraints will match to the query portion of a URI that will
   * be exactly the same as another logically equivalent URI.
   * (where, for example, the query parameters may be re-ordered)
   *
   * If the constraints are {@link Constraints#isUnbounded() unbounded} a special case
   * of "unbounded" will be returned.
   *
   * @return a normalized query string for the specified constraints.
   *
   * @since 1.1
   */
  public static String getNormalizedConstraints(Constraints constraints) {
    // the constraints map isn't naturally ordered
    // we want to ensure that our output is

    if (constraints.isUnbounded()) {
      // unbounded constrains is a special case, here we just use
      // "unbounded" as the constraint
      return UNBOUNDED_CONSTRAINT;
    }

    Map<String, String> orderedConstraints = constraints.toNormalizedQueryMap();

    List<String> parts = new ArrayList<String>();
    // build a query portion of the URI
    for (Map.Entry<String, String> entry : orderedConstraints.entrySet()) {
      StringBuilder builder = new StringBuilder();
      String key = entry.getKey();
      String value = entry.getValue();
      builder.append(key);
      builder.append("=");
      if (value != null) {
        builder.append(value);
      }
      parts.add(builder.toString());
    }

    return Joiner.on('&').join(parts);
  }

}
