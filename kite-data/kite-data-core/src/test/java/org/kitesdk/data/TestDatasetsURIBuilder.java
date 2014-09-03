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

import java.net.URI;

import org.junit.Test;

import junit.framework.Assert;

public class TestDatasetsURIBuilder {
  @Test
  public void testBuildDatasetUri() {
    URI uri = new URIBuilder("repo:file:/tmp/data", "ns", "test-ds").build();
    Assert.assertEquals(URI.create("dataset:file:/tmp/data/ns/test-ds"), uri);
  }
  
  @Test
  public void testBuildViewUri() {
    URI uri = new URIBuilder("repo:file:/tmp/data", "ns", "test-ds")
        .with("username", "bob").with("lastName", "Smith").build();
    Assert.assertEquals(URI.create("view:file:/tmp/data/ns/test-ds?username=bob&lastName=Smith"), uri);
  }
  
  @Test
  public void testRepoUriHasQueryString() {
    URI uri = new URIBuilder("repo:file:/tmp/data?repoParam=x", "ns", "test-ds")
        .with("username", "bob")
        .build();
    
    Assert.assertEquals(URI.create("view:file:/tmp/data/ns/test-ds?repoParam=x&username=bob"), uri);
  }
}
