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
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemUtil {

  private static final Logger LOG = LoggerFactory.getLogger(FileSystemUtil.class);

  /**
   * Creates, if necessary, the given the location for {@code descriptor}.
   *
   * @param conf A Configuration
   * @param descriptor A DatasetDescriptor
   * @throws DatasetIOException
   * @since 0.13.0
   */
  public static void ensureLocationExists(
      DatasetDescriptor descriptor, Configuration conf) {
    Preconditions.checkNotNull(descriptor.getLocation(),
        "Cannot get FileSystem for a descriptor with no location");

    Path dataPath = new Path(descriptor.getLocation());
    FileSystem fs = null;

    try {
      fs = dataPath.getFileSystem(conf);
    } catch (IOException e) {
      throw new DatasetIOException(
          "Cannot get FileSystem for descriptor: " + descriptor, e);
    }

    try {
      if (!fs.exists(dataPath)) {
        fs.mkdirs(dataPath);
      }
    } catch (IOException e) {
      throw new DatasetIOException("Cannot access data location", e);
    }
  }

  static boolean cleanlyDelete(FileSystem fs, Path root, Path path) {
    Preconditions.checkNotNull(fs, "File system cannot be null");
    Preconditions.checkNotNull(root, "Root path cannot be null");
    Preconditions.checkNotNull(path, "Path to delete cannot be null");
    try {
      boolean deleted;
      // attempt to relativize the path to delete
      Path relativePath;
      if (path.isAbsolute()) {
        relativePath = new Path(root.toUri().relativize(path.toUri()));
      } else {
        relativePath = path;
      }

      if (relativePath.isAbsolute()) {
        // path is not relative to the root. delete just the path
        LOG.debug("Deleting path {}", path);
        deleted = fs.delete(path, true /* include any files */ );
      } else {
        // the is relative to the root path
        Path absolute = new Path(root, relativePath);
        LOG.debug("Deleting path {}", absolute);
        deleted = fs.delete(absolute, true /* include any files */ );
        // iterate up to the root, removing empty directories
        for (Path current = absolute.getParent();
             !current.equals(root) && !(current.getParent() == null);
             current = current.getParent()) {
          final FileStatus[] stats = fs.listStatus(current);
          if (stats == null || stats.length == 0) {
            // dir is empty and should be removed
            LOG.debug("Deleting empty path {}", current);
            deleted = fs.delete(current, true) || deleted;
          } else {
            // all parent directories will be non-empty
            break;
          }
        }
      }
      return deleted;
    } catch (IOException ex) {
      throw new DatasetIOException("Could not cleanly delete path:" + path, ex);
    }
  }
}
