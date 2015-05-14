/*
 * Copyright 2013 Cloudera.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spi.filesystem;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.spi.StorageKey;

public class PathIterator implements Iterator<Path>, Iterable<Path> {
  private final FileSystem fs;
  private final Path root;
  private final Iterator<StorageKey> partitions;
  private StorageKey key = null;
  private Iterator<Path> files = null;

  public PathIterator(FileSystem fs, Path root,
                      @Nullable Iterator<StorageKey> partitions) {
    this.fs = fs;
    this.root = root;
    this.partitions = partitions;
  }

  @Override
  public boolean hasNext() {
    if (files != null && files.hasNext()) {
      return true;
    } else {
      return advance();
    }
  }

  /**
   * Returns the StorageKey for the current file Path.
   *
   * Must be called after next().
   *
   * @return a StorageKey for the current file Path
   */
  public StorageKey getStorageKey() {
    return key;
  }

  @Override
  public Path next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    return files.next();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @SuppressWarnings("deprecation")
  private boolean advance() {
    while (true) {
      FileStatus[] stats;
      if (partitions == null) {
        if (files != null) {
          // already read the root directory
          return false;
        }
        try {
          stats = fs.listStatus(root, PathFilters.notHidden());
        } catch (IOException ex) {
          throw new DatasetIOException("Cannot list files in " + root, ex);
        }
        this.key = null;

      } else if (partitions.hasNext()) {
        StorageKey key = partitions.next();
        try {
          stats = fs.listStatus(
              new Path(root, key.getPath()), PathFilters.notHidden());
        } catch (IOException ex) {
          throw new DatasetIOException("Cannot list files in " + key.getPath(), ex);
        }
        this.key = key;

      } else {
        return false;
      }

      List<Path> nextFileSet = Lists.newArrayListWithCapacity(stats.length);
      for (FileStatus stat : stats) {
        if (!stat.isDir()) {
          nextFileSet.add(stat.getPath());
        }
      }
      if (nextFileSet.size() > 0) {
        this.files = nextFileSet.iterator();
        return true;
      }
      return false;
    }
  }

  @Override
  public Iterator<Path> iterator() {
    return this;
  }
}

