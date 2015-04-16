/*
 * Copyright 2015 Cloudera Inc.
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

package org.kitesdk.data.spi.s3;

import com.google.common.collect.Lists;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.spi.DefaultConfiguration;

public class TestS3Dataset {
  private static final String ID = System.getProperty("test.aws.access.key");
  private static final String KEY = System.getProperty("test.aws.s3.secret.key");
  private static final String BUCKET = System.getProperty("test.aws.s3.bucket");

  private static Configuration original = null;

  @BeforeClass
  public static void addCredentials() {
    original = DefaultConfiguration.get();
    Configuration conf = DefaultConfiguration.get();
    if (ID != null) {
      conf.set("fs.s3n.awsAccessKeyId", ID);
      conf.set("fs.s3n.awsSecretAccessKey", KEY);
      conf.set("fs.s3a.access.key", ID);
      conf.set("fs.s3a.secret.key", KEY);
    }
    DefaultConfiguration.set(conf);
  }

  @AfterClass
  public static void resetConfiguration() {
    DefaultConfiguration.set(original);
  }

  @Test
  public void testBasics3n() {
    // only run this test if credentials are present
    Assume.assumeTrue(ID != null && !ID.isEmpty());

    String uri = "dataset:s3n://" + BUCKET + "/ns/test";

    // make sure the dataset doesn't already exist
    Datasets.delete(uri);

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral("\"string\"")
        .build();

    Dataset<String> dataset = Datasets.create(uri, descriptor, String.class);

    List<String> expected = Lists.newArrayList("a", "b", "time");
    DatasetWriter<String> writer = null;
    try {
      writer = dataset.newWriter();
      for (String s : expected) {
        writer.write(s);
      }
    } finally {
      if (writer != null) {
        writer.close();
      }
    }

    DatasetReader<String> reader = null;
    try {
      reader = dataset.newReader();
      Assert.assertEquals("Should match written strings",
          expected, Lists.newArrayList((Iterator<String>) reader));
    } finally {
      if (reader != null) {
        reader.close();
      }
    }

    // clean up
    Datasets.delete(uri);
  }

  @Test
  public void testBasics3a() {
    // only run this test if credentials are present
    Assume.assumeTrue(ID != null && !ID.isEmpty());

    String uri = "dataset:s3a://" + BUCKET + "/ns/test";

    // make sure the dataset doesn't already exist
    Datasets.delete(uri);

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaLiteral("\"string\"")
        .build();

    Dataset<String> dataset = Datasets.create(uri, descriptor, String.class);

    List<String> expected = Lists.newArrayList("a", "b", "time");
    DatasetWriter<String> writer = null;
    try {
      writer = dataset.newWriter();
      for (String s : expected) {
        writer.write(s);
      }
    } finally {
      if (writer != null) {
        writer.close();
      }
    }

    DatasetReader<String> reader = null;
    try {
      reader = dataset.newReader();
      Assert.assertEquals("Should match written strings",
          expected, Lists.newArrayList((Iterator<String>) reader));
    } finally {
      if (reader != null) {
        reader.close();
      }
    }

    // clean up
    Datasets.delete(uri);
  }
}
