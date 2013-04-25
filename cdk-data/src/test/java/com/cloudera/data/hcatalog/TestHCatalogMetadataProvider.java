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
package com.cloudera.data.hcatalog;

import com.cloudera.data.DatasetDescriptor;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestHCatalogMetadataProvider {

  private FileSystem fileSystem;
  private Path testDirectory;

  @Before
  public void setUp() throws IOException {
    fileSystem = FileSystem.get(new Configuration());
    testDirectory = new Path(Files.createTempDir().getAbsolutePath());
  }

  @Test
  public void testNonPartitioned() throws IOException {
    HCatalogMetadataProvider provider = new HCatalogMetadataProvider(false);

    Schema userSchema = new Schema.Parser().parse(Resources.getResource(
        "schema/user.avsc").openStream());

    provider.setFileSystem(fileSystem);
    provider.setDataDirectory(testDirectory);
    provider.save("test", new DatasetDescriptor.Builder().schema(userSchema)
        .get());

    DatasetDescriptor descriptor = provider.load("test");

    Assert.assertNotNull(descriptor);
    Assert.assertEquals(userSchema, descriptor.getSchema());
    Assert.assertFalse(descriptor.isPartitioned());

    boolean result = provider.delete("test");
    Assert.assertTrue(result);

    result = provider.delete("test");
    Assert.assertFalse(result);
  }

}
