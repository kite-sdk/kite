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

package org.kitesdk.data.filesystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * <p>
 * A {@link org.kitesdk.data.MetadataProvider} that stores dataset metadata in a 
 * Hadoop {@link org.apache.hadoop.fs.FileSystem}.
 * </p>
 * <p>
 * When configured with a root directory, this implementation serializes the
 * information within a {@link org.kitesdk.data.DatasetDescriptor} on the 
 * provided {@link org.apache.hadoop.fs.FileSystem}. The descriptor is 
 * serialized as an Avro object and stored in a directory named after the 
 * dataset name. For example, if the dataset name is {@code logs}, the directory 
 * {@code rootDirectory/logs/} is created if it doesn't exist, and the 
 * serialized descriptor is stored in the file {@code descriptor.avro}.
 * </p>
 * @deprecated will be removed in 0.14.0; moved to SPI.
 */
@Deprecated
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value="NM_SAME_SIMPLE_NAME_AS_SUPERCLASS",
    justification="Replaced by parent class")
public class FileSystemMetadataProvider extends
    org.kitesdk.data.spi.filesystem.FileSystemMetadataProvider {
  public FileSystemMetadataProvider(Configuration conf, Path rootDirectory) {
    super(conf, rootDirectory);
  }

  /**
   * A fluent builder to aid in the construction of 
   * {@link FileSystemMetadataProvider} instances.
   * @since 0.8.0
   * @deprecated will be removed in 0.14.0
   */
  @Deprecated
  public static class Builder {

    private Path rootDirectory;
    private Configuration configuration;

    /**
     * The root directory for metadata files.
     *
     * @param path a Path to a FileSystem location
     * @return this Builder for method chaining.
     */
    @SuppressWarnings("deprecation")
    public Builder rootDirectory(Path path) {
      this.rootDirectory = path;
      return this;
    }

    /**
     * The {@link Configuration} used to find the
     * {@link org.apache.hadoop.fs.FileSystem}.
     */
    @SuppressWarnings("deprecation")
    public Builder configuration(Configuration configuration) {
      this.configuration = configuration;
      return this;
    }

    /**
     * Build an instance of the configured {@link FileSystemMetadataProvider}.
     *
     * @since 0.9.0
     */
    @SuppressWarnings("deprecation")
    public FileSystemMetadataProvider build() {
      return new FileSystemMetadataProvider(configuration, rootDirectory);
    }
  }
}
