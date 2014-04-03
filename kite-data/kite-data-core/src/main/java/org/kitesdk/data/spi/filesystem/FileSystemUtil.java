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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetIOException;

public class FileSystemUtil {

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
}
