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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class TemporaryFileSystemDatasetRepository extends FileSystemDatasetRepository
    implements TemporaryDatasetRepository {

  private static final Logger LOG = LoggerFactory
      .getLogger(FileSystemDatasetRepository.class);

  private static final String TEMP_PATH = ".temp";
  private final FileSystem fs;
  private final Path root;
  private final Path storage;

  public TemporaryFileSystemDatasetRepository(Configuration conf, Path root,
      String namespace, String key) {
    super(conf, temporaryRoot(root, namespace, key));
    try {
      this.fs = root.getFileSystem(conf);
      this.root = fs.makeQualified(root);
      this.storage = fs.makeQualified(temporaryRoot(root, namespace, key));
    } catch (IOException e) {
      throw new DatasetIOException(
          "Cannot setup temporary repository: " + key, e);
    }
    LOG.debug("Created temporary dataset repository with root {} and storage {}.", root,
        storage);
  }

  private static Path temporaryRoot(Path root, String namespace, String key) {
    return new Path(root, new Path(namespace, new Path(TEMP_PATH, key)));
  }

  public void delete() {
    FileSystemUtil.cleanlyDelete(fs, root, storage);
    LOG.debug("Deleted temporary dataset repository with storage {}.", storage);
  }
}
