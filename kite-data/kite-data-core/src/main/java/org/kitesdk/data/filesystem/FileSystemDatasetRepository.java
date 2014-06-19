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
 * A {@link org.kitesdk.data.DatasetRepository} that stores data in a Hadoop
 * {@link org.apache.hadoop.fs.FileSystem}.
 * </p>
 * <p>
 * Given a {@link org.apache.hadoop.fs.FileSystem}, a root directory, and a
 * {@link org.kitesdk.data.spi.MetadataProvider},
 * {@link org.kitesdk.data.DatasetRepository} loads and stores
 * {@link org.kitesdk.data.Dataset}s on both local filesystems and the Hadoop
 * Distributed FileSystem (HDFS). You can  instantiate this class directly with 
 * the three dependencies above, then perform dataset-related operations using 
 * any of the provided methods. The primary methods of interest are
 * {@link #create(String, org.kitesdk.data.DatasetDescriptor)},
 * {@link #load(String)}, and
 * {@link #delete(String)} which create a new dataset, load an existing
 * dataset, or delete an existing dataset, respectively. Once you create or load
 *  a dataset, you can invoke the appropriate {@link org.kitesdk.data.Dataset} 
 * methods to get a reader or writer as needed.
 * </p>
 * <p>
 * {@link org.kitesdk.data.DatasetWriter} instances returned from this
 * implementation have the following <code>flush()</code> method semantics.
 * For Avro files, <code>flush()</code> invokes HDFS <code>hflush</code>,
 * which guarantees that client buffers are flushed, so new readers see all
 * entries written up to that point. For Parquet files, <code>flush()</code> 
 * has no effect.
 * </p>
 *
 * @see org.kitesdk.data.DatasetRepository
 * @see org.kitesdk.data.Dataset
 * @see org.kitesdk.data.DatasetDescriptor
 * @see org.kitesdk.data.PartitionStrategy
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value="NM_SAME_SIMPLE_NAME_AS_SUPERCLASS",
    justification="Replaced by parent class")
public class FileSystemDatasetRepository extends
    org.kitesdk.data.spi.filesystem.FileSystemDatasetRepository {
  /**
   * This is included to avoid compiler complaints.
   */
  FileSystemDatasetRepository(Configuration conf, Path rootDirectory) {
    super(conf, rootDirectory);
  }

  /**
   * A fluent builder to aid in the construction of {@link FileSystemDatasetRepository}
   * instances.
   * @since 0.2.0
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="NM_SAME_SIMPLE_NAME_AS_SUPERCLASS",
      justification="Replaced by parent class")
  public static class Builder extends
      org.kitesdk.data.spi.filesystem.FileSystemDatasetRepository.Builder {
  }
}

