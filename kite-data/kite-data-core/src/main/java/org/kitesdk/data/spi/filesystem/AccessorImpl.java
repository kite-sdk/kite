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

import java.util.Iterator;

import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetException;
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

}
