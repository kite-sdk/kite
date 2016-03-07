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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.DatasetDescriptor;

public class TestAvroWriterWithoutRename extends TestAvroWriter {
  @Override
  public FileSystemWriter<Record> newWriter(Path directory, Schema datasetSchema, Schema writerSchema) {
    return FileSystemWriter.newWriter(fs, directory, 100, 2 * 1024 * 1024,
        new DatasetDescriptor.Builder()
            .property(
                "kite.writer.roll-interval-seconds", String.valueOf(10))
            .property(
                "kite.writer.target-file-size",
                String.valueOf(32 * 1024 * 1024)) // 32 MB
            .property(
                "kite.writer.fs-supports-rename", String.valueOf(false))
            .schema(datasetSchema)
            .format("avro")
            .build(), writerSchema);
  }
}
