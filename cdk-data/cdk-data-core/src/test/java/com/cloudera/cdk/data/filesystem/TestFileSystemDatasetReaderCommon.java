/*
 * Copyright 2013 Cloudera.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.cdk.data.filesystem;

import com.cloudera.cdk.data.DatasetReader;
import static com.cloudera.cdk.data.filesystem.DatasetTestUtilities.STRING_SCHEMA;
import com.google.common.io.Resources;
import java.io.IOException;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;

public class TestFileSystemDatasetReaderCommon extends TestDatasetReaderCommon {

  @Override
  public DatasetReader newReader() throws IOException {
    return new FileSystemDatasetReader<String>(
        FileSystem.getLocal(new Configuration()),
        new Path(Resources.getResource("data/strings-100.avro").getFile()),
        STRING_SCHEMA);
  }

  @Override
  public int getTotalRecords() {
    return 100;
  }

  @Override
  public DatasetTestUtilities.RecordValidator getValidator() {
    return new DatasetTestUtilities.RecordValidator<GenericData.Record>() {
      @Override
      public void validate(GenericData.Record record, int recordNum) {
        Assert.assertEquals(String.valueOf(recordNum), record.get("text"));
      }
    };
  }

}
