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

package org.kitesdk.data.spi;

import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.data.ColumnMapping;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.PartitionStrategy;

public class TestDescriptorUtil {
  @Test
  public void testRoundTripWithUri() throws IOException {
    // copy the user avsc file to the local FS
    File schemaFile = new File("target/user.avsc");
    FileOutputStream out = new FileOutputStream(schemaFile);
    ByteStreams.copy(Resources.getResource("schema/user.avsc").openStream(), out);
    out.close();

    DatasetDescriptor original = new DatasetDescriptor.Builder()
        .schemaUri("file:" + schemaFile.getAbsolutePath())
        .partitionStrategy(new PartitionStrategy.Builder()
            .identity("email")
            .build())
        .columnMapping(new ColumnMapping.Builder()
            .key("email")
            .column("username", "u", "n")
            .build())
        .location("file:/tmp/datasets/ns/my_dataset")
        .property("test.property", "value")
        .build();

    Configuration conf = new Configuration(false); // fresh config

    DescriptorUtil.addToConfiguration(original, "context", conf);

    Assert.assertNotNull("Should use kite.context.* properties",
        conf.get("kite.context.schema-uri"));

    DatasetDescriptor reconstructed = DescriptorUtil.buildFromConfiguration(
        conf, "context");

    Assert.assertEquals("Reconstructed configuration should match original",
        original, reconstructed);
  }

  @Test
  public void testRoundTripWithLiteral() throws IOException {
    // resource URIs are not preserved because they depend on the classpath
    DatasetDescriptor original = new DatasetDescriptor.Builder()
        .schemaUri("resource:schema/user.avsc")
        .partitionStrategy(new PartitionStrategy.Builder()
            .identity("email")
            .build())
        .columnMapping(new ColumnMapping.Builder()
            .key("email")
            .column("username", "u", "n")
            .build())
        .location("file:/tmp/datasets/ns/my_dataset")
        .property("test.property", "value")
        .build();

    Configuration conf = new Configuration(false); // fresh config

    DescriptorUtil.addToConfiguration(original, "context", conf);

    Assert.assertNotNull("Should use kite.context.* properties",
        conf.get("kite.context.schema-literal"));

    DatasetDescriptor reconstructed = DescriptorUtil.buildFromConfiguration(
        conf, "context");

    Assert.assertEquals("Reconstructed configuration should match original",
        original, reconstructed);
  }
}
