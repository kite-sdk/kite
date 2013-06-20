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
package com.cloudera.cdk.data;

import com.cloudera.cdk.data.DatasetDescriptor;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URI;
import junit.framework.Assert;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import static com.cloudera.cdk.data.filesystem.DatasetTestUtilities.*;

public class TestDatasetDescriptor {

  @Test
  public void testSchemaFromHdfs() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      // start HDFS cluster
      cluster = new MiniDFSCluster.Builder(new Configuration()).build();
      FileSystem fs = cluster.getFileSystem();

      // copy a schema to HDFS
      Path schemaPath = new Path("schema.avsc").makeQualified(fs);
      FSDataOutputStream out = fs.create(schemaPath);
      IOUtils.copyBytes(USER_SCHEMA_URL.toURL().openStream(), out, fs.getConf());
      out.close();

      // build a schema using the HDFS path and check it's the same
      Schema schema = new DatasetDescriptor.Builder().schema(schemaPath.toUri()).get()
          .getSchema();

      Assert.assertEquals(USER_SCHEMA, schema);

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testSchemaFromAvroDataFile() throws Exception {
    URI uri = Resources.getResource("data/strings-100.avro").toURI();
    Schema schema = new DatasetDescriptor.Builder().schemaFromAvroDataFile(uri).get()
        .getSchema();
    Assert.assertEquals(STRING_SCHEMA, schema);
  }
}
