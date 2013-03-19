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
package com.cloudera.data.filesystem;

import com.cloudera.data.DatasetDescriptor;
import com.google.common.io.Resources;
import junit.framework.Assert;
import org.apache.avro.Schema;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;

public class TestDatasetDescriptor {

  @Test
  public void testSchemaFromAvroDataFile() throws IOException {
    URL url = Resources.getResource("data/strings-100.avro");
    Schema schema = new DatasetDescriptor.Builder().schemaFromAvroDataFile(url).get()
        .getSchema();
    Schema expectedSchema = new Schema.Parser().parse(
        Resources.getResource("schema/string.avsc").openStream());
    Assert.assertEquals(expectedSchema, schema);
  }
}
