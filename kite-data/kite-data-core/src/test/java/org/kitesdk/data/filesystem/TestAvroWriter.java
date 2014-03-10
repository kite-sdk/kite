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

package org.kitesdk.data.filesystem;

import org.apache.hadoop.fs.Path;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetWriter;

public class TestAvroWriter extends TestFileSystemWriters<Object> {
  @Override
  public DatasetWriter<Object> newWriter(Path directory) {
    return new FileSystemWriter<Object>(fs, directory,
        new DatasetDescriptor.Builder()
            .schemaLiteral("\"string\"")
            .format("avro")
            .build());
  }
}
