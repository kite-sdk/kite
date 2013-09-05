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
import com.google.common.collect.Lists;
import java.io.IOException;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import static com.cloudera.cdk.data.filesystem.TestMultiFileDatasetReader.*;

public class TestMultiFileDatasetReaderCommon extends TestDatasetReaderCommon {

  @Override
  public DatasetReader newReader() throws IOException {
    return new MultiFileDatasetReader<GenericData.Record>(
        FileSystem.get(new Configuration()),
        Lists.newArrayList(TEST_FILE, TEST_FILE),
        DESCRIPTOR);
  }

  @Override
  public int getTotalRecords() {
    return 200;
  }

  @Override
  public DatasetTestUtilities.RecordValidator getValidator() {
    return VALIDATOR;
  }

}
