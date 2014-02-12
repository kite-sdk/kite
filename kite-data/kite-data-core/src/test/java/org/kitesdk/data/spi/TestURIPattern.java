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

package org.kitesdk.data.spi;

import com.google.common.collect.Maps;
import java.net.URISyntaxException;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestURIPattern {

  private Map<String, String> expected = null;

  @Before
  public void setup() {
    expected = Maps.newHashMap();
  }

  @Test
  public void testNonAbsoluteExactMatch() throws URISyntaxException {
    URIPattern pattern = new URIPattern("hive");
    String uri = "hive";
    Assert.assertTrue(pattern.matches(uri));
  }

  @Test
  public void testSubURI() throws URISyntaxException {
    URIPattern pattern = new URIPattern("scheme:*sub-uri");
    String uri = "scheme:other-scheme:/path/to/data.avro";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("scheme", "scheme");
    expected.put("sub-uri", "other-scheme:/path/to/data.avro");
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testStaticMatch() throws URISyntaxException {
    URIPattern pattern = new URIPattern("scheme:static");
    String uri = "scheme:static";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("scheme", "scheme");
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testPathGlob() throws URISyntaxException {
    URIPattern pattern = new URIPattern("file:///*path");
    String uri = "file:/path/to/data.avro";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("scheme", "file");
    expected.put("path", "path/to/data.avro");
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testPathVariables() throws URISyntaxException {
    URIPattern pattern = new URIPattern("mysql:/:db/:table");
    String uri = "mysql:/myDB/myTable";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("scheme", "mysql");
    expected.put("db", "myDB");
    expected.put("table", "myTable");
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testEmptyPath() throws URISyntaxException {
    URIPattern pattern = new URIPattern("scheme://authority");
    String uri = "scheme://real-authority";

    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("scheme", "scheme");
    expected.put("host", "real-authority");
    Assert.assertEquals(expected, actual);

    // should not match any path element or fragment
    Assert.assertFalse(pattern.matches(uri + "/"));
    Assert.assertFalse(pattern.matches("scheme:/"));
    Assert.assertFalse(pattern.matches(uri + "/abc"));
    Assert.assertFalse(pattern.matches(uri + "#fragment"));
  }

  @Test
  public void testStaticPathStart() throws URISyntaxException {
    URIPattern pattern = new URIPattern("mysql:/required/:db/:table");
    String uri = "mysql:/required/myDB/myTable";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("scheme", "mysql");
    expected.put("db", "myDB");
    expected.put("table", "myTable");
    Assert.assertEquals(expected, actual);

    Assert.assertNull(pattern.getMatch("mysql:/myDB/myTable"));
  }

  @Test
  public void testStaticPathMixed() throws URISyntaxException {
    URIPattern pattern = new URIPattern("mysql:/db/:db/table/:table");
    String uri = "mysql:/db/myDB/table/myTable";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("scheme", "mysql");
    expected.put("db", "myDB");
    expected.put("table", "myTable");
    Assert.assertEquals(expected, actual);

    Assert.assertNull(pattern.getMatch("mysql:/myDB/myTable"));
  }

  @Test
  public void testStaticPathEnd() throws URISyntaxException {
    URIPattern pattern = new URIPattern("mysql:/:db/:table/wtf");
    String uri = "mysql:/myDB/myTable/wtf";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("scheme", "mysql");
    expected.put("db", "myDB");
    expected.put("table", "myTable");
    Assert.assertEquals(expected, actual);

    Assert.assertNull(pattern.getMatch("mysql:/myDB/myTable"));
  }

  @Test
  public void testPathVariablesWithGlob() throws URISyntaxException {
    URIPattern pattern = new URIPattern("mysql:/:db/:table/*the-rest");
    String uri = "mysql:/myDB/myTable/a/b/c";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("scheme", "mysql");
    expected.put("db", "myDB");
    expected.put("table", "myTable");
    expected.put("the-rest", "a/b/c");
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testDefaultAuthority() throws URISyntaxException {
    URIPattern pattern = new URIPattern("scheme://user:pass:w0rd@host:3434/*path");
    String uri = "scheme:/path/to/data.avro";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("scheme", "scheme");
    expected.put("host", "host");
    expected.put("port", "3434");
    expected.put("username", "user");
    expected.put("password", "pass:w0rd");
    expected.put("path", "path/to/data.avro");
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testOverrideAuthority() throws URISyntaxException {
    URIPattern pattern = new URIPattern("scheme://user:pass:w0rd@host:3434/*path");
    String uri = "scheme://other:3435/path/to/data.avro";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("scheme", "scheme");
    expected.put("host", "other");
    expected.put("port", "3435");
    expected.put("username", "user");
    expected.put("password", "pass:w0rd");
    expected.put("path", "path/to/data.avro");
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testDefaultQueryArgs() throws URISyntaxException {
    URIPattern pattern = new URIPattern("scheme:/*path?custom-option=true&use-ssl=false");
    String uri = "scheme:/path/to/data.avro";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("scheme", "scheme");
    expected.put("path", "path/to/data.avro");
    expected.put("custom-option", "true");
    expected.put("use-ssl", "false");
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testOverrideQueryArgs() throws URISyntaxException {
    URIPattern pattern = new URIPattern("scheme:/*path?custom-option=true&use-ssl=false");
    String uri = "scheme:/path/to/data.avro?use-ssl=true";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("scheme", "scheme");
    expected.put("path", "path/to/data.avro");
    expected.put("custom-option", "true");
    expected.put("use-ssl", "true");
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testMixed() throws URISyntaxException {
    URIPattern pattern = new URIPattern(
        "mysql://cloudera:cloudera@db-host:3434/:db/:table/*the-rest?custom-option=true&use-ssl=false");
    String uri =
        "mysql://real-db-host/my-db/my-table?use-ssl=true";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("scheme", "mysql");
    expected.put("host", "real-db-host");
    expected.put("port", "3434");
    expected.put("username", "cloudera");
    expected.put("password", "cloudera");
    expected.put("db", "my-db");
    expected.put("table", "my-table");
    expected.put("custom-option", "true");
    expected.put("use-ssl", "true");
    Assert.assertEquals(expected, actual);
  }

  // This is common in this type of matching, but the query part prevents it
//  @Test
//  public void testPathVariablesOptional() throws URISyntaxException {
//    URIPattern pattern = new URIPattern("mysql:/:db/:table?");
//    String uri = "mysql:/myDB/myTable";
//    Assert.assertTrue(pattern.matches(uri));
//    Map<String, String> actual = pattern.getMatch(uri);
//    expected.put("db", "myDB");
//    expected.put("table", "myTable");
//    Assert.assertEquals(expected, actual);
//
//    uri = "mysql:/myDB";
//    Assert.assertTrue(pattern.matches(uri));
//    actual = pattern.getMatch(uri);
//    expected.put("scheme", "mysql");
//    expected.put("db", "myDB");
//    expected.put("table", "myTable");
//    Assert.assertEquals(expected, actual);
//  }

}
