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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.TestHelpers;

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
    expected.put("uri:scheme", "scheme");
    expected.put("sub-uri", "other-scheme:/path/to/data.avro");
    Assert.assertEquals(expected, actual);

    URI constructed = pattern.construct(expected);
    Assert.assertEquals(URI.create(uri), constructed);
  }

  @Test
  public void testStaticMatch() throws URISyntaxException {
    URIPattern pattern = new URIPattern("scheme:static");
    String uri = "scheme:static";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("uri:scheme", "scheme");
    Assert.assertEquals(expected, actual);

    URI constructed = pattern.construct(expected);
    Assert.assertEquals(URI.create(uri), constructed);
  }

  @Test
  public void testPathGlob() throws URISyntaxException {
    URIPattern pattern = new URIPattern("file:///*path");
    String uri = "file:/path/to/data.avro";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("uri:scheme", "file");
    expected.put("path", "path/to/data.avro");
    Assert.assertEquals(expected, actual);

    URI constructed = pattern.construct(expected);
    Assert.assertEquals(URI.create(uri), constructed);
  }

  @Test
  public void testPathVariables() throws URISyntaxException {
    URIPattern pattern = new URIPattern("mysql:/:db/:table");
    String uri = "mysql:/myDB/myTable";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("uri:scheme", "mysql");
    expected.put("db", "myDB");
    expected.put("table", "myTable");
    Assert.assertEquals(expected, actual);

    URI constructed = pattern.construct(expected);
    Assert.assertEquals(URI.create(uri), constructed);
  }

  @Test
  public void testEmptyPath() throws URISyntaxException {
    URIPattern pattern = new URIPattern("scheme://authority");
    String uri = "scheme://real-authority";

    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("uri:scheme", "scheme");
    expected.put("auth:host", "real-authority");
    Assert.assertEquals(expected, actual);

    // should not match any path element or fragment
    Assert.assertFalse(pattern.matches(uri + "/"));
    Assert.assertFalse(pattern.matches("scheme:/"));
    Assert.assertFalse(pattern.matches(uri + "/abc"));
    Assert.assertFalse(pattern.matches(uri + "#fragment"));

    URI constructed = pattern.construct(expected);
    Assert.assertEquals(URI.create(uri), constructed);
  }

  @Test
  public void testStaticPathStart() throws URISyntaxException {
    URIPattern pattern = new URIPattern("mysql:/required/:db/:table");
    String uri = "mysql:/required/myDB/myTable";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("uri:scheme", "mysql");
    expected.put("db", "myDB");
    expected.put("table", "myTable");
    Assert.assertEquals(expected, actual);

    Assert.assertNull(pattern.getMatch("mysql:/myDB/myTable"));

    URI constructed = pattern.construct(expected);
    Assert.assertEquals(URI.create(uri), constructed);
  }

  @Test
  public void testStaticPathMixed() throws URISyntaxException {
    URIPattern pattern = new URIPattern("mysql:/db/:db/table/:table");
    String uri = "mysql:/db/myDB/table/myTable";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("uri:scheme", "mysql");
    expected.put("db", "myDB");
    expected.put("table", "myTable");
    Assert.assertEquals(expected, actual);

    Assert.assertNull(pattern.getMatch("mysql:/myDB/myTable"));

    URI constructed = pattern.construct(expected);
    Assert.assertEquals(URI.create(uri), constructed);
  }

  @Test
  public void testStaticPathEnd() throws URISyntaxException {
    URIPattern pattern = new URIPattern("mysql:/:db/:table/wtf");
    String uri = "mysql:/myDB/myTable/wtf";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("uri:scheme", "mysql");
    expected.put("db", "myDB");
    expected.put("table", "myTable");
    Assert.assertEquals(expected, actual);

    Assert.assertNull(pattern.getMatch("mysql:/myDB/myTable"));

    URI constructed = pattern.construct(expected);
    Assert.assertEquals(URI.create(uri), constructed);
  }

  @Test
  public void testIgnoreTrailingSlash() throws URISyntaxException {
    URIPattern pattern = new URIPattern("mysql:/:db/:table");
    String uri = "mysql:/myDB/myTable/";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("uri:scheme", "mysql");
    expected.put("db", "myDB");
    expected.put("table", "myTable");
    Assert.assertEquals(expected, actual);

    URI constructed = pattern.construct(expected);
    Assert.assertEquals(URI.create("mysql:/myDB/myTable"), constructed);
  }

  @Test
  public void testPathVariablesWithGlob() throws URISyntaxException {
    URIPattern pattern = new URIPattern("mysql:/:db/:table/*the-rest");
    String uri = "mysql:/myDB/myTable/a/b/c";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("uri:scheme", "mysql");
    expected.put("db", "myDB");
    expected.put("table", "myTable");
    expected.put("the-rest", "a/b/c");
    Assert.assertEquals(expected, actual);

    URI constructed = pattern.construct(expected);
    Assert.assertEquals(URI.create(uri), constructed);
  }

  @Test
  public void testPathVariablesAfterGlob() throws URISyntaxException {
    URIPattern pattern = new URIPattern("file:/*path/:ns/:ds");
    String uri = "file:/a/b/c/namespace/dataset";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("uri:scheme", "file");
    expected.put("ns", "namespace");
    expected.put("ds", "dataset");
    expected.put("path", "a/b/c");
    Assert.assertEquals(expected, actual);

    URI constructed = pattern.construct(expected);
    Assert.assertEquals(URI.create(uri), constructed);
  }

  @Test
  public void testOpaquePathVariablesAfterGlob() throws URISyntaxException {
    URIPattern pattern = new URIPattern("file:*path/:ns/:ds");
    String uri = "file:a/b/c/namespace/dataset";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("uri:scheme", "file");
    expected.put("ns", "namespace");
    expected.put("ds", "dataset");
    expected.put("path", "a/b/c");
    Assert.assertEquals(expected, actual);

    URI constructed = pattern.construct(expected);
    Assert.assertEquals(URI.create(uri), constructed);
  }

  @Test
  public void testPathVariablesBeforeAndAfterGlob() throws URISyntaxException {
    URIPattern pattern = new URIPattern("file:/:ns/*path/:ds");
    String uri = "file:/namespace/a/b/c/dataset";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("uri:scheme", "file");
    expected.put("ns", "namespace");
    expected.put("ds", "dataset");
    expected.put("path", "a/b/c");
    Assert.assertEquals(expected, actual);

    URI constructed = pattern.construct(expected);
    Assert.assertEquals(URI.create(uri), constructed);
  }

  @Test
  public void testOpaquePathVariablesBeforeAndAfterGlob() throws URISyntaxException {
    URIPattern pattern = new URIPattern("file::ns/*path/:ds");
    String uri = "file:namespace/a/b/c/dataset";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("uri:scheme", "file");
    expected.put("ns", "namespace");
    expected.put("ds", "dataset");
    expected.put("path", "a/b/c");
    Assert.assertEquals(expected, actual);

    URI constructed = pattern.construct(expected);
    Assert.assertEquals(URI.create(uri), constructed);
  }

  @Test
  public void testDefaultAuthority() throws URISyntaxException {
    URIPattern pattern = new URIPattern("scheme://user:pass:w0rd@host:3434/*path");
    String uri = "scheme:/path/to/data.avro";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("uri:scheme", "scheme");
    expected.put("auth:host", "host");
    expected.put("auth:port", "3434");
    expected.put("auth:username", "user");
    expected.put("auth:password", "pass:w0rd");
    expected.put("path", "path/to/data.avro");
    Assert.assertEquals(expected, actual);

    URI constructed = pattern.construct(expected);
    Assert.assertEquals(URI.create(uri), constructed);
  }

  @Test
  public void testOverrideAuthority() throws URISyntaxException {
    URIPattern pattern = new URIPattern("scheme://user:pass:w0rd@host:3434/*path");
    String uri = "scheme://other:3435/path/to/data.avro";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("uri:scheme", "scheme");
    expected.put("auth:host", "other");
    expected.put("auth:port", "3435");
    expected.put("auth:username", "user");
    expected.put("auth:password", "pass:w0rd");
    expected.put("path", "path/to/data.avro");
    Assert.assertEquals(expected, actual);

    URI constructed = pattern.construct(expected);
    Assert.assertEquals(URI.create(uri), constructed);
  }

  @Test
  public void testDefaultQueryArgs() throws URISyntaxException {
    URIPattern pattern = new URIPattern("scheme:/*path?custom-option=true&use-ssl=false&encoded=a%2Cb");
    String uri = "scheme:/path/to/data.avro";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("uri:scheme", "scheme");
    expected.put("path", "path/to/data.avro");
    expected.put("custom-option", "true");
    expected.put("use-ssl", "false");
    expected.put("encoded", "a%2Cb");
    Assert.assertEquals(expected, actual);

    URI constructed = pattern.construct(expected);
    Assert.assertEquals(URI.create(uri), constructed);
  }

  @Test
  public void testOpaqueDefaultQueryArgs() throws URISyntaxException {
    URIPattern pattern = new URIPattern("scheme:*path?custom-option=true&use-ssl=false");
    String uri = "scheme:path/to/data.avro";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("uri:scheme", "scheme");
    expected.put("path", "path/to/data.avro");
    expected.put("custom-option", "true");
    expected.put("use-ssl", "false");
    Assert.assertEquals(expected, actual);

    URI constructed = pattern.construct(expected);
    Assert.assertEquals(URI.create(uri), constructed);
  }

  @Test
  public void testOverrideQueryArgs() throws URISyntaxException {
    URIPattern pattern = new URIPattern("scheme:/*path?custom-option=true&use-ssl=false");
    String uri = "scheme:/path/to/data.avro?use-ssl=true";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("uri:scheme", "scheme");
    expected.put("path", "path/to/data.avro");
    expected.put("custom-option", "true");
    expected.put("use-ssl", "true");
    Assert.assertEquals(expected, actual);

    URI constructed = pattern.construct(expected);
    Assert.assertEquals(URI.create(uri), constructed);
  }

  @Test
  public void testOpaqueOverrideQueryArgs() throws URISyntaxException {
    URIPattern pattern = new URIPattern("scheme:*path?custom-option=true&use-ssl=false");
    String uri = "scheme:path/to/data.avro?use-ssl=true";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("uri:scheme", "scheme");
    expected.put("path", "path/to/data.avro");
    expected.put("custom-option", "true");
    expected.put("use-ssl", "true");
    Assert.assertEquals(expected, actual);

    URI constructed = pattern.construct(expected);
    Assert.assertEquals(URI.create(uri), constructed);
  }

  @Test
  public void testMixed() throws URISyntaxException {
    URIPattern pattern = new URIPattern(
        "mysql://cloudera:cloudera@db-host:3434/:db/:table/*the-rest?custom-option=true&use-ssl=false");
    String uri =
        "mysql://real-db-host/my-db/my-table?use-ssl=true";
    Assert.assertTrue(pattern.matches(uri));

    Map<String, String> actual = pattern.getMatch(uri);
    expected.put("uri:scheme", "mysql");
    expected.put("auth:host", "real-db-host");
    expected.put("auth:port", "3434");
    expected.put("auth:username", "cloudera");
    expected.put("auth:password", "cloudera");
    expected.put("db", "my-db");
    expected.put("table", "my-table");
    expected.put("custom-option", "true");
    expected.put("use-ssl", "true");
    Assert.assertEquals(expected, actual);

    URI constructed = pattern.construct(expected);
    Assert.assertEquals(URI.create(uri), constructed);
  }

  @Test
  public void testConstructMissingOptions() throws URISyntaxException {
    final URIPattern pattern = new URIPattern("scheme:/*path/:name");
    TestHelpers.assertThrows("Should reject missing name",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            Map<String, String> options = Maps.newHashMap();
            options.put("path", "a/b/c");
            pattern.construct(options);
          }
        });

    // globs are optional
    Map<String, String> options = Maps.newHashMap();
    options.put("name", "table-name");
    URI constructed = pattern.construct(options);
    Assert.assertEquals(URI.create("scheme:/table-name"), constructed);
  }

  @Test
  public void testNotEnoughPathComponents() throws URISyntaxException {
    URIPattern pattern = new URIPattern("scheme:*path/:ns/:name");
    Assert.assertFalse(pattern.matches("scheme://host:3434/dataset"));
  }

  @Test
  public void testEmptyGlobs() throws URISyntaxException {
    URIPattern pattern = new URIPattern("file:/a/*path");
    Map<String, String> match = pattern.getMatch("file:/a");
    Assert.assertNotNull(match);
    Assert.assertNull(match.get("path"));

    match = pattern.getMatch("file:/a/");
    Assert.assertNotNull(match);
    Assert.assertEquals("", match.get("path"));

    match = pattern.getMatch("file:/a//");
    Assert.assertNotNull(match);
    Assert.assertEquals("/", match.get("path"));

    pattern = new URIPattern("file:/a/*path/:dataset");
    match = pattern.getMatch("file:/a//dataset");
    Assert.assertNotNull(match);
    Assert.assertEquals("", match.get("path"));

    match = pattern.getMatch("file:/a/dataset");
    Assert.assertNotNull(match);
    Assert.assertNull(match.get("path"));

    pattern = new URIPattern("file:/*path");
    match = pattern.getMatch("file:/");
    Assert.assertNotNull(match);
    Assert.assertNull(match.get("path"));
  }

  @Test
  public void testQueryEncoding() throws URISyntaxException {
    URIPattern pattern = new URIPattern("file:/path");
    URI original = URI.create("file:/path?f1=a,b,a%2Cb");
    Map<String, String> match = pattern.getMatch(original);
    Assert.assertNotNull(match);
    Assert.assertEquals("a,b,a%2Cb", match.get("f1"));
    URI constructed = pattern.construct(match);
    Assert.assertEquals(original, constructed);
  }

  @Test
  public void testOpaqueQueryEncoding() throws URISyntaxException {
    URIPattern pattern = new URIPattern("file:path");
    URI original = URI.create("file:path?f1=a,b,a%2Cb");
    Map<String, String> match = pattern.getMatch(original);
    Assert.assertNotNull(match);
    Assert.assertEquals("a,b,a%2Cb", match.get("f1"));
    URI constructed = pattern.construct(match);
    Assert.assertEquals(original, constructed);
  }

  @Test
  public void testPathEncoding() throws URISyntaxException {
    URIPattern pattern = new URIPattern("file:/*path");
    URI original = URI.create("file:/a%2Fb/c");
    Map<String, String> match = pattern.getMatch(original);
    Assert.assertNotNull(match);
    Assert.assertEquals("a%2Fb/c", match.get("path"));
    URI constructed = pattern.construct(match);
    Assert.assertEquals(original, constructed);
  }

  @Test
  public void testOpaquePathEncoding() throws URISyntaxException {
    URIPattern pattern = new URIPattern("file:*path");
    URI original = URI.create("file:a%2Fb/c");
    Map<String, String> match = pattern.getMatch(original);
    Assert.assertNotNull(match);
    Assert.assertEquals("a%2Fb/c", match.get("path"));
    URI constructed = pattern.construct(match);
    Assert.assertEquals(original, constructed);
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
