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

import java.io.IOException;
import javax.annotation.concurrent.Immutable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.spi.TemporaryDatasetRepository;

@Immutable
public class TemporaryFileSystemDatasetRepository extends FileSystemDatasetRepository
    implements TemporaryDatasetRepository {
  private static final String tempPath = ".temp";
  private final FileSystem fs;
  private final Path root;
  private final Path storage;

  public TemporaryFileSystemDatasetRepository(Configuration conf, Path root, String key) {
    super(conf, new Path(root, new Path(tempPath, key)));
    try {
      this.fs = root.getFileSystem(conf);
      this.root = fs.makeQualified(root);
      this.storage = fs.makeQualified(new Path(root, new Path(tempPath, key)));
    } catch (IOException e) {
      throw new DatasetIOException(
          "Cannot setup temporary repository: " + key, e);
    }
  }

  public void delete() {
    FileSystemUtil.cleanlyDelete(fs, root, storage);
  }
}
