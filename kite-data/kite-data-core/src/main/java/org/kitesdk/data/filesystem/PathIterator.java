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

package org.kitesdk.data.filesystem;

import org.kitesdk.data.DatasetException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

class PathIterator implements Iterator<Path>, Iterable<Path> {
  private final FileSystem fs;
  private final Iterator<Path> directories;
  private Iterator<Path> files = null;

  public PathIterator(FileSystem fs, Iterable<Path> directories) {
    Preconditions.checkArgument(directories != null,
        "Directories cannot be null");

    this.fs = fs;
    this.directories = directories.iterator();
  }

  @Override
  public boolean hasNext() {
    if (files != null && files.hasNext()) {
      return true;
    } else {
      return advance();
    }
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

  private boolean advance() {
    while (true) {
      if (directories.hasNext()) {
        final Path directory = directories.next();
        try {
          final FileStatus[] stats = fs.listStatus(directory, PathFilters.notHidden());
          final List<Path> nextFileSet = Lists.newArrayListWithCapacity(stats.length);
          for (FileStatus stat : stats) {
            if (stat.isFile()) {
              nextFileSet.add(stat.getPath());
            }
          }
          if (nextFileSet.size() > 0) {
            this.files = nextFileSet.iterator();
            return true;
          }
        } catch (IOException ex) {
          throw new DatasetException("Cannot list files in " + directory, ex);
        }
      } else {
        return false;
      }
    }
  }

  @Override
  public Iterator<Path> iterator() {
    return this;
  }
}

