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

import com.google.common.annotations.VisibleForTesting;

public class FileSystemProperties {
  /**
   * Used to avoid the cost of durable parquet writes when guarantees are
   * handled by another layer, like an
   * {@link org.apache.hadoop.mapreduce.OutputCommitter}.
   *
   * The value should be a boolean.
   */
  public static final String NON_DURABLE_PARQUET_PROP = "kite.parquet.non-durable-writes";

  /**
   * Used to control the size of the writer cache when writing to multiple
   * partitions.
   *
   * The value should be an integer.
   */
  public static final String WRITER_CACHE_SIZE_PROP = "kite.writer.cache-size";

  /**
   * Used to enable CSV writing; for testing only.
   *
   * The value should be a boolean.
   */
  @VisibleForTesting
  static final String ALLOW_CSV_PROP = "kite.allow.csv";

  /**
   * Used to enable record reuse, if supported by the implementation.
   */
  public static final String REUSE_RECORDS = "kite.reader.reuse-records";

  /**
   * Used to set the target size, in bytes, for data files. Data files will be
   * closed and finalized once they reach this size.
   */
  public static final String TARGET_FILE_SIZE_PROP = "kite.writer.target-file-size";

  /**
   * Used to set the roll interval, in seconds, for data files. Data files will
   * be closed and finalized once they reach this age.
   */
  public static final String ROLL_INTERVAL_S_PROP = "kite.writer.roll-interval-seconds";

  /**
   * Until HADOOP-9565 is available and fully adopted, need to make this configurable so that
   * we can avoid multiple expensive copy operations when writing output to a file system that
   * does not support fast file renaming. Default is false unless the FileSystem URI scheme is
   * s3n or s3a.
   */
  public static final String OBJECTSTORE_FILESYSTEM = "kite.writer.object-store";
}
