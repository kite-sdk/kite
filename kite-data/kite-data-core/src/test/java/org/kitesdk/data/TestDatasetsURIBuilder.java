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
package org.kitesdk.data;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDatasetsURIBuilder {
  private static FileSystem localFS;

  @BeforeClass
  public static void beforeClass() throws Exception {
    localFS = FileSystem.getLocal(new Configuration());
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:schema/user.avsc")
        .build();
    DatasetRepository repo = DatasetRepositories.open("repo:file:/tmp/data");
    repo.create("test-ds", descriptor);
  }
  
  @AfterClass
  public static void afterClass() throws IOException {
    localFS.delete(new Path("/tmp/data"), true);
  }
  
  @Test
  public void testBuildDatasetUri() {
    String uri = new Datasets.URIBuilder("repo:file:/tmp/data", "test-ds").build();
    Assert.assertEquals("dataset:file:/tmp/data/test-ds", uri);
  }
  
  @Test
  public void testBuildViewUri() {
    String uri = new Datasets.URIBuilder("repo:file:/tmp/data", "test-ds")
        .with("username", "bob").build();
    Assert.assertEquals("view:file:/tmp/data/test-ds?username=bob", uri);
  }
}
