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
package org.kitesdk.data.spi.filesystem;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.PartitionKey;
import org.kitesdk.data.View;
import org.apache.hadoop.fs.Path;

final class AccessorImpl {

  public Path getDirectory(Dataset<?> dataset) {
    if (dataset instanceof FileSystemDataset) {
      return ((FileSystemDataset<?>) dataset).getDirectory();
    }
    return null;
  }

  public Iterator<Path> getDirectoryIterator(View view) {
    if (view instanceof FileSystemView) {
      return ((FileSystemView<?>) view).dirIterator();
    } else if (view instanceof FileSystemDataset) {
      return ((FileSystemDataset<?>) view).dirIterator();
    } else {
      throw new DatasetException(
          "Underlying Dataset must be a FileSystemDataset");
    }
  }

  public <E> PartitionKey fromDirectoryName(Dataset<E> dataset, Path dir) {
    if (dataset instanceof FileSystemDataset) {
      return ((FileSystemDataset<E>) dataset).keyFromDirectory(dir);
    }
    return null;
  }

  @Override
  public <E> long getSize(View<E> view) throws IOException {
    if (view instanceof FileSystemView || view instanceof FileSystemDataset) {
      FileSystem fs = ((FileSystemDataset) view.getDataset()).getFileSystem();
      long size = 0;
      for (Iterator<Path> i = getDirectoryIterator(view); i.hasNext(); ) {
        Path dir = i.next();
        for (FileStatus st : fs.listStatus(dir)) {
          size += st.getLen();
        }
      }
      return size;
    }
    return -1;
  }

  @Override
  public <E> long getLastModified(View<E> view) throws IOException {
    if (view instanceof FileSystemView || view instanceof FileSystemDataset) {
      FileSystem fs = ((FileSystemDataset) view.getDataset()).getFileSystem();
      long lastMod = -1;
      for (Iterator<Path> i = getDirectoryIterator(view); i.hasNext(); ) {
        Path dir = i.next();
        for (FileStatus st : fs.listStatus(dir)) {
          if (lastMod < st.getModificationTime()) {
            lastMod = st.getModificationTime();
          }
        }
      }
      return lastMod;
    }
    return -1;
  }

}
