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

package com.cloudera.cdk.data.filesystem;

import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.DatasetWriter;
import com.cloudera.cdk.data.DatasetWriterException;
import com.cloudera.cdk.data.Format;
import com.cloudera.cdk.data.Formats;
import com.cloudera.cdk.data.UnknownFormatException;
import com.google.common.base.Joiner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

abstract class FileSystemWriters {

  @SuppressWarnings("unchecked") // See https://github.com/Parquet/parquet-mr/issues/106
  public static <E> DatasetWriter<E> newFileWriter(
      FileSystem fs, Path path, DatasetDescriptor descriptor) {
    // ensure the path exists
    try {
      fs.mkdirs(path);
    } catch (IOException ex) {
      throw new DatasetWriterException("Could not create path:" + path, ex);
    }

    final Format format = descriptor.getFormat();
    final Path file = new Path(path, uniqueFilename(descriptor.getFormat()));

    if (Formats.PARQUET.equals(format)) {
      return new ParquetFileSystemDatasetWriter(fs, file, descriptor.getSchema());
    } else if (Formats.AVRO.equals(format)) {
      return new FileSystemDatasetWriter.Builder()
          .fileSystem(fs)
          .path(file)
          .schema(descriptor.getSchema())
          .build();
    } else {
      throw new UnknownFormatException("Unknown format:" + format);
    }
  }

  private static Joiner DASH = Joiner.on('-');

  private static String uniqueFilename(Format format) {
    // FIXME: This file name is not guaranteed to be truly unique.
    return DASH.join(
        System.currentTimeMillis(),
        Thread.currentThread().getId() + "." + format.getExtension());
  }

}
