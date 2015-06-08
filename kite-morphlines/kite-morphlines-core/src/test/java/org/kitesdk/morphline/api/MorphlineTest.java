/*
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
package org.kitesdk.morphline.api;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.junit.Ignore;
import org.junit.Test;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.FaultTolerance;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Metrics;
import org.kitesdk.morphline.base.Notifications;
import org.kitesdk.morphline.shaded.com.google.code.regexp.Matcher;
import org.kitesdk.morphline.shaded.com.google.code.regexp.Pattern;
import org.kitesdk.morphline.shaded.com.google.common.reflect.ClassPath;
import org.kitesdk.morphline.shaded.com.google.common.reflect.ClassPath.ResourceInfo;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigUtil;

public class MorphlineTest extends AbstractMorphlineTest {

  private void processAndVerifySuccess(Record input, Record expected) {
    processAndVerifySuccess(input, expected, true);
  }

  private void processAndVerifySuccess(Record input, Record expected, boolean isSame) {
    collector.reset();
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(input));
    assertEquals(expected, collector.getFirstRecord());
    if (isSame) {
      assertSame(input, collector.getFirstRecord());    
    } else {
      assertNotSame(input, collector.getFirstRecord());    
    }
  }

  private void processAndVerifySuccess(Record input, Multimap... expectedMaps) {
    collector.reset();
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(input));    
    Iterator<Record> iter = collector.getRecords().iterator();
    for (Multimap expected : expectedMaps) {
      assertTrue(iter.hasNext());
      Record record = iter.next();
      assertEquals(expected, record.getFields());
    }    
    assertFalse(iter.hasNext());
  }

  private void processAndVerifyFailure(Record input) {
    collector.reset();
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertFalse(morphline.process(input));
    assertEquals(0, collector.getRecords().size());    
  }

  @Test
  public void testMorphlineContext() throws Exception {
    ExceptionHandler ex = new ExceptionHandler() {      
      @Override
      public void handleException(Throwable t, Record record) {
        throw new RuntimeException(t);        
      }
    };
    
    MetricRegistry metricRegistry = new MetricRegistry();
    metricRegistry.register("myCounter", new Counter());
    
    HealthCheckRegistry healthChecks = new HealthCheckRegistry();
    healthChecks.register("foo", new HealthCheck() {      
      @Override
      protected Result check() throws Exception {
        return Result.healthy("flawless");
      }
    });

    Map<String,Object> settings = new HashMap<String,Object>(); 
    
    MorphlineContext ctx = new MorphlineContext.Builder()
      .setSettings(settings)
      .setExceptionHandler(ex)
      .setHealthCheckRegistry(healthChecks)
      .setMetricRegistry(metricRegistry)
      .build();
    
    assertSame(settings, ctx.getSettings());
    assertSame(ex, ctx.getExceptionHandler());
    assertSame(metricRegistry, ctx.getMetricRegistry());
    assertSame(healthChecks, ctx.getHealthCheckRegistry());
    ctx.getHealthCheckRegistry().runHealthChecks();
    
    assertEquals(0, new MorphlineContext.Builder().build().getSettings().size());
  }
  
  @Test
  public void testParseComplexConfig() throws Exception {
    parse("test-morphlines/parseComplexConfig");
    parse("test-morphlines/tutorialReadAvroContainer");
    parse("test-morphlines/tutorialReadJsonTestTweets");
  }
  
  @Test
  public void testParseInclude() throws Exception {
    morphline = createMorphline("test-morphlines/parseInclude");  
    Record input = new Record();
    Record expected = new Record();
    expected.put("result",  "bar");
    processAndVerifySuccess(input, expected);
  }
  
  @Test
  public void testParseIncludeConstants() throws Exception {
    morphline = createMorphline("test-morphlines/parseIncludeConstants");  
    Record input = new Record();
    Record expected = new Record();
    expected.put("myField",  "foo");
    processAndVerifySuccess(input, expected);
  }
  
  @Test
  public void testParseVariables() throws Exception {
    System.setProperty("ENV_ZK_HOST", "zk.foo.com:2181/solr");
    System.setProperty("ENV_SOLR_URL", "http://foo.com:8983/solr/myCollection");
    System.setProperty("ENV_SOLR_LOCATOR", "{ collection : collection1 }");
    try {
      Config override = ConfigFactory.parseString("SOLR_LOCATOR : { collection : fallback } ");
      Config config = parse("test-morphlines/parseVariables", override);
      //System.out.println(config.root().render());
    } finally {
      System.clearProperty("ENV_ZK_HOST");
      System.clearProperty("ENV_SOLR_URL");  
      System.clearProperty("ENV_SOLR_LOCATOR");
    }
  }
  
  @Test
  public void testPipeWithTwoBasicCommands() throws Exception {
    morphline = createMorphline("test-morphlines/pipeWithTwoBasicCommands");    
    Record record = createBasicRecord();
    processAndVerifySuccess(record, record);
  }
  
  @Test
  public void testRemoveFields() throws Exception {
    morphline = createMorphline("test-morphlines/removeFields");
    for (int i = 0; i < 2; i++) {
      Record record = new Record();
      record.put("foo", "data");
      record.put("foobar", "data");
      record.put("barx", "data");
      record.put("barox", "data");
      record.put("baz", "data");
      record.put("baz", "data");
      record.put("hello", "data");
      
      Record expected = new Record();
      expected.put("foobar", "data");
      expected.put("barox", "data");
      expected.put("hello", "data");
      processAndVerifySuccess(record, expected);
    }
  }

  @Test
  public void testRemoveFieldsWithLiteralsOnly() throws Exception {
    morphline = createMorphline("test-morphlines/removeFieldsWithLiteralsOnly");
    for (int i = 0; i < 2; i++) {
      Record record = new Record();
      record.put("foo", "data");
      record.put("baz", "data");
      record.put("baz", "data");
      record.put("hello", "data");
      
      Record expected = new Record();
      expected.put("foo", "data");
      expected.put("hello", "data");
      processAndVerifySuccess(record, expected);
    }
  }

  @Test
  public void testRemoveAllFields() throws Exception {
    morphline = createMorphline("test-morphlines/removeAllFields");    
    Record record = new Record();
    record.put("foo", "data");
    processAndVerifySuccess(record, new Record());
  }

  @Test(expected=IllegalArgumentException.class)
  public void testRemoveFieldsSyntaxErrorWithMissingColon() throws Exception {  
    createMorphline("test-morphlines/removeFieldsSyntaxErrorWithMissingColon");
  }

  @Test(expected=IllegalArgumentException.class)
  public void testRemoveFieldsSyntaxErrorWithUnknownType() throws Exception {  
    createMorphline("test-morphlines/removeFieldsSyntaxErrorWithUnknownType");
  }

  @Test
  public void testRemoveValues() throws Exception {
    morphline = createMorphline("test-morphlines/removeValues");
    for (int i = 0; i < 2; i++) {
      Record record = new Record();
      record.put("foobar", "data");
      record.put("foo", "foo");
      record.put("foo", "foobar");
      record.put("foo", "barx");
      record.put("foo", "barox");
      record.put("foo", "baz");
      record.put("foo", "baz");
      record.put("foo", "hello");
      record.put("barx", "foo");
      record.put("barox", "foo");
      record.put("baz", "foo");
      record.put("baz", "foo");
      record.put("hello", "foo");
      
      Record expected = new Record();
      expected.put("foobar", "data");
      expected.put("foo", "foobar");
      expected.put("foo", "barox");
      expected.put("foo", "hello");
      expected.put("barox", "foo");
      expected.put("hello", "foo");
      processAndVerifySuccess(record, expected);
    }
  }

  @Test
  public void testRemoveValuesWithLiteralsOnly() throws Exception {
    morphline = createMorphline("test-morphlines/removeValuesWithLiteralsOnly");
    for (int i = 0; i < 2; i++) {
      Record record = new Record();
      record.put("foo", "foo");
      record.put("foo", "baz");
      record.put("bar", "bar");
      record.put("baz", "baz");
      record.put("baz", "xxxx");
      record.put("hello", "data");
      
      Record expected = new Record();
      expected.put("foo", "foo");
      expected.put("foo", "baz");
      expected.put("bar", "bar");
      expected.put("hello", "data");
      processAndVerifySuccess(record, expected);
    }
  }

  @Test
  public void testReplaceValues() throws Exception {
    morphline = createMorphline("test-morphlines/replaceValues");
    for (int i = 0; i < 2; i++) {
      Record record = new Record();
      record.put("foobar", "data");
      record.put("foo", "foo");
      record.put("foo", "foobar");
      record.put("foo", "barx");
      record.put("foo", "barox");
      record.put("foo", "baz");
      record.put("foo", "baz");
      record.put("foo", "hello");
      record.put("barx", "foo");
      record.put("barox", "foo");
      record.put("baz", "foo");
      record.put("baz", "foo");
      record.put("hello", "foo");
      
      Record expected = new Record();
      expected.put("foobar", "data");
      expected.put("foo", "myReplacement");
      expected.put("foo", "foobar");
      expected.put("foo", "myReplacement");
      expected.put("foo", "barox");
      expected.put("foo", "myReplacement");
      expected.put("foo", "myReplacement");
      expected.put("foo", "hello");
      expected.put("barx", "myReplacement");
      expected.put("barox", "foo");
      expected.put("baz", "myReplacement");
      expected.put("baz", "myReplacement");
      expected.put("hello", "foo");
      processAndVerifySuccess(record, expected);
    }
  }

  @Test
  public void testReplaceValuesWithLiteralsOnly() throws Exception {
    morphline = createMorphline("test-morphlines/replaceValuesWithLiteralsOnly");
    for (int i = 0; i < 2; i++) {
      Record record = new Record();
      record.put("foo", "foo");
      record.put("foo", "baz");
      record.put("bar", "bar");
      record.put("baz", "baz");
      record.put("baz", "xxxx");
      record.put("hello", "data");
      
      Record expected = new Record();
      expected.put("foo", "foo");
      expected.put("foo", "baz");
      expected.put("bar", "bar");
      expected.put("baz", "myReplacement");
      expected.put("baz", "myReplacement");
      expected.put("hello", "data");
      processAndVerifySuccess(record, expected);
    }
  }

  @Test
  public void testNotifications() throws Exception {
    morphline = createMorphline("test-morphlines/pipeWithTwoBasicCommands");
    Notifications.notifyBeginTransaction(morphline);
    Notifications.notifyStartSession(morphline);
    Notifications.notifyCommitTransaction(morphline);
    Notifications.notifyRollbackTransaction(morphline);
  }
  
  @Test
  public void testCompile() throws Exception {
    String file = "test-morphlines/pipeWithTwoBasicCommands";
    morphline = new Compiler().compile(
        new File(RESOURCES_DIR + "/" + file + ".conf"), 
        "", 
        new MorphlineContext.Builder().build(), 
        null);
    assertNotNull(morphline);
    
    new Fields();
    new Metrics();    
  }
  
  @Test
  public void testCompileWithExplicitMorphlineId() throws Exception {
    String file = "test-morphlines/pipeWithTwoBasicCommands";
    morphline = new Compiler().compile(
        new File(RESOURCES_DIR + "/" + file + ".conf"), 
        "morphline1", 
        new MorphlineContext.Builder().build(), 
        null);
    assertNotNull(morphline);
  }
  
  @Test
  public void testCompileWithUnknownMorphlineId() throws Exception {
    String file = "test-morphlines/pipeWithTwoBasicCommands";
    try {
      new Compiler().compile(
        new File(RESOURCES_DIR + "/" + file + ".conf"), 
        "morphline2", 
        new MorphlineContext.Builder().build(), 
        null);
      fail();
    } catch (MorphlineCompilationException e) {
      ; // expected
    }
  }
  
  @Test
  public void testCompileWithMissingMorphline() throws Exception {
    String file = "test-morphlines/compileWithMissingMorphline";
    try {
      new Compiler().compile(
          new File(RESOURCES_DIR + "/" + file + ".conf"), 
          "morphline1", 
          new MorphlineContext.Builder().build(), 
          null);
      fail();
    } catch (MorphlineCompilationException e) {
      ; // expected
    }
  }
  
  @Test
  public void testFaultTolerance() throws Exception {
    FaultTolerance tolerance = new FaultTolerance(true, false);
    tolerance = new FaultTolerance(true, true, IOException.class.getName());
    tolerance.handleException(new IOException(), new Record());
    
    tolerance = new FaultTolerance(true, true, IOException.class.getName());
    tolerance.handleException(new RuntimeException(), new Record());

    tolerance = new FaultTolerance(true, true, IOException.class.getName());
    tolerance.handleException(new RuntimeException(new IOException()), new Record());

    tolerance = new FaultTolerance(true, false, IOException.class.getName());
    try {
      tolerance.handleException(new IOException(), new Record());
      fail();
    } catch (MorphlineRuntimeException e) {
      ; // expected
    }

    tolerance = new FaultTolerance(false, false, IOException.class.getName());
    try {
      tolerance.handleException(new IOException(), new Record());
      fail();
    } catch (MorphlineRuntimeException e) {
      ; // expected
    }

    tolerance = new FaultTolerance(false, false, IOException.class.getName());
    try {
      tolerance.handleException(new RuntimeException(), new Record());
      fail();
    } catch (MorphlineRuntimeException e) {
      ; // expected
    }
    
    tolerance = new FaultTolerance(true, true, Error.class.getName());
    try {
      tolerance.handleException(new Error(), new Record());
      fail();
    } catch (Error e) {
      ; // expected
    }
  }
  
  @Test
  public void testLog() throws Exception {
    morphline = createMorphline("test-morphlines/log");    
    Record record = createBasicRecord();
    processAndVerifySuccess(record, record);
  }

  @Test
  public void testAddValues() throws Exception {
    morphline = createMorphline("test-morphlines/addValues");    
    Record record = new Record();
    record.put("first_name", "Nadja");
    Record expected = new Record();
    expected.put("first_name", "Nadja");
    expected.put("source_type", "text/log");
    expected.put("source_type", "text/log2");
    expected.put("source_host", "123");
    expected.put("name", "Nadja");
    expected.put("names", "@{first_name}");
    expected.put("pids", 456);
    expected.put("pids", "hello");
    processAndVerifySuccess(record, expected);
  }

  @Test
  public void testAddValuesIfAbsent() throws Exception {
    morphline = createMorphline("test-morphlines/addValuesIfAbsent");    
    Record record = new Record();
    Record expected = new Record();
    expected.put("source_type", "text/log");
    processAndVerifySuccess(record, expected);
  }

  @Test
  public void testAddValuesIfAbsentWithLargeN() throws Exception {
    morphline = createMorphline("test-morphlines/addValuesIfAbsentWithLargeN");    
    Record record = new Record();
    record.put("source_type", "tag2");
    Record expected = new Record();
    expected.put("source_type", "tag2");
    expected.put("source_type", "tag1");
    expected.put("source_type", "tag3");
    expected.put("source_type", "tag4");
    expected.put("source_type", "tag5");
    processAndVerifySuccess(record, expected);
  }

  @Test
  public void testSetValues() throws Exception {
    morphline = createMorphline("test-morphlines/setValues");    
    Record record = new Record();
    record.put("first_name", "Nadja");
    record.put("source_type", "XXXX");
    record.put("source_type", "XXXX");
    record.put("source_host", 999);
    record.put("name", "XXXX");
    record.put("names", "XXXX");
    record.put("pids", 789);
    record.put("pids", "YYYY");

    Record expected = new Record();
    expected.put("first_name", "Nadja");
    expected.put("source_type", "text/log");
    expected.put("source_type", "text/log2");
    expected.put("source_host", "123");
    expected.put("name", "Nadja");
    expected.put("names", "@{first_name}");
    expected.put("pids", 456);
    expected.put("pids", "hello");
    
    processAndVerifySuccess(record, expected);
  }

  @Test
  public void testAddCurrentTime() throws Exception {
    morphline = createMorphline("test-morphlines/addCurrentTime");  
    Record record = new Record();
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    long now = System.currentTimeMillis();
    assertTrue(morphline.process(record));
    assertSame(record, collector.getFirstRecord());
    long actual = ((Long) record.getFirstValue("ts")).longValue();
    assertTrue(actual >= now);
    assertTrue(actual <= now + 1000);
    
    // test that preserveExisting = true preserves the existing timestamp
    collector.reset();
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    Thread.sleep(1);
    assertTrue(morphline.process(record));
    assertSame(record, collector.getFirstRecord());
    long actual2 = ((Long) record.getFirstValue("ts")).longValue();
    assertEquals(actual, actual2);
  }

  @Test
  public void testAddLocalHost() throws Exception {
    morphline = createMorphline("test-morphlines/addLocalHost");  
    InetAddress localHost;
    try {
      localHost = InetAddress.getLocalHost();
    } catch (UnknownHostException e) {
      return;
    }
    
    testAddLocalHostInternal(localHost.getHostAddress());
  }

  @Test
  public void testAddLocalHostWithName() throws Exception {
    morphline = createMorphline("test-morphlines/addLocalHostWithName");
    InetAddress localHost;
    try {
      localHost = InetAddress.getLocalHost();
    } catch (UnknownHostException e) {
      return;
    }
    
    testAddLocalHostInternal(localHost.getCanonicalHostName());
  }

  private void testAddLocalHostInternal(String name) throws Exception {
    Record record = new Record();
    Record expected = new Record();
    expected.put("myhost", name);
    processAndVerifySuccess(record, expected);
    
    // test that preserveExisting = true preserves the existing value
    record = new Record();
    record.put("myhost", "myname");
    expected = record.copy();
    processAndVerifySuccess(record, expected);
  }

  @Test
  public void testToByteArray() throws Exception {
    morphline = createMorphline("test-morphlines/toByteArray");    
    Record record = new Record();
    record.put("first_name", "Nadja");    
    collector.reset();
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    assertEquals(1, collector.getFirstRecord().getFields().size());
    byte[] expected = "Nadja".getBytes("UTF-8");
    assertArrayEquals(expected, (byte[]) collector.getFirstRecord().getFirstValue("first_name"));
    assertSame(record, collector.getFirstRecord());    
  }

  @Test
  public void testToString() throws Exception {
    morphline = createMorphline("test-morphlines/toString");    
    Record record = new Record();
    record.put("first_name", "Nadja");
    
    Record expected = new Record();
    expected.put("first_name", "Nadja");
    expected.put("source_type", "text/log");
    expected.put("source_type", "text/log2");
    expected.put("source_host", "123");
    expected.put("name", "Nadja");
    expected.put("names", "@{first_name}");
    expected.put("pids", "456");
    expected.put("pids", "hello");

    processAndVerifySuccess(record, expected);
  }

  @Test
  public void testToStringWithTrim() throws Exception {
    morphline = createMorphline("test-morphlines/toStringWithTrim");    
    Record record = new Record();
    Record expected = new Record();
    expected.put("source_type", "hello world");
    expected.put("source_host", " hello world ");
    processAndVerifySuccess(record, expected);
  }

  @Test
  public void testCharacterEscaping() throws Exception {
    morphline = createMorphline("test-morphlines/characterEscaping");    
    Record record = new Record();
    Record expected = new Record();
    expected.put("foo", "\t");
    expected.put("foo", "\n");
    expected.put("foo", "\r");
    expected.put("foo", "\\.");    
    expected.put("foo", String.valueOf((char)1));    
    expected.put("foo", "byte[]");    
    processAndVerifySuccess(record, expected);
  }

  @Test
  public void testEqualsSuccess() throws Exception {
    morphline = createMorphline("test-morphlines/equalsSuccess");    
    Record record = new Record();
    record.put("first_name", "Nadja");
//    record.put("field0", null);
    record.put("field1", "true");
//    record.put("field2", 123);
    record.put("field3", "123");
    record.put("field4", "123");
    record.put("field4", 456);
    record.put("field5", "Nadja");    
    processAndVerifySuccess(record, record);
  }
  
  @Test
  /* Fails because Boolean.TRUE is not equals to the String "true" */
  public void testEqualsFailure() throws Exception {
    morphline = createMorphline("test-morphlines/equalsFailure");    
    Record record = new Record();
    record.put("field0", true);    
    processAndVerifyFailure(record);
  }

  @Test
  public void testContainsSuccess() throws Exception {
    morphline = createMorphline("test-morphlines/contains");    
    Record record = new Record();
    record.put("food", "veggies");
    record.put("food", "cookie");    
    processAndVerifySuccess(record, record);
  }

  @Test
  public void testContainsFailure() throws Exception {
    morphline = createMorphline("test-morphlines/contains");    
    Record record = new Record();
    record.put("food", "veggies");
    record.put("food", "xxxxxxxxxxxxxx");   
    processAndVerifyFailure(record);
  }

  @Test
  public void testHead() throws Exception {
    morphline = createMorphline("test-morphlines/head");
    int size = 5;
    for (int i = 0; i < size; i++) {
      assertTrue(morphline.process(new Record()));
    }
    assertEquals(3, collector.getRecords().size());    
  }
  
  @Test
  public void testHeadUnlimited() throws Exception {
    morphline = createMorphline("test-morphlines/headUnlimited");
    int size = 5;
    for (int i = 0; i < size; i++) {
      assertTrue(morphline.process(new Record()));
    }
    assertEquals(size, collector.getRecords().size());    
  }
  
  @Test
  public void testSampleWithSeed() throws Exception {
    morphline = createMorphline("test-morphlines/sampleWithSeed");
    int size = 10;
    for (int i = 0; i < size; i++) {
      assertTrue(morphline.process(new Record()));
    }
    assertEquals(9, collector.getRecords().size());    
  }
  
  @Test
  public void testSampleWithoutSeed() throws Exception {
    morphline = createMorphline("test-morphlines/sampleWithoutSeed");
    int size = 100;
    for (int i = 0; i < size; i++) {
      assertTrue(morphline.process(new Record()));
    }
    int actual = collector.getRecords().size();
    double delta = 0.2;
    assertTrue("actual: " + actual, actual >= Math.round(size * (0.9 - delta)));
    assertTrue("actual: " + actual, actual <= Math.round(size * (0.9 + delta)));
  }
  
  @Test
  public void testSampleWithProbabilityOne() throws Exception {
    morphline = createMorphline("test-morphlines/sampleWithProbabilityOne");
    int size = 10;
    for (int i = 0; i < size; i++) {
      assertTrue(morphline.process(new Record()));
    }
    assertEquals(size, collector.getRecords().size());    
  }
  
  @Test
  public void testSeparateAttachments() throws Exception {
    morphline = createMorphline("test-morphlines/separateAttachments");    
    Record record = new Record();
    record.put(Fields.ATTACHMENT_BODY, "a_foo");
    record.put(Fields.ATTACHMENT_BODY, "a_bar");
    record.put(Fields.ATTACHMENT_BODY, "a_baz");
    
    record.put(Fields.ATTACHMENT_MIME_TYPE, "m_foo");
    record.put(Fields.ATTACHMENT_MIME_TYPE, "m_bar");

    record.put(Fields.ATTACHMENT_CHARSET, "c_foo");
    
    record.put(Fields.ATTACHMENT_NAME, "n_foo");

    record.put("first_name", "Nadja");
    
    processAndVerifySuccess(record, 
      ImmutableMultimap.of("first_name", "Nadja", Fields.ATTACHMENT_BODY, "a_foo",
        Fields.ATTACHMENT_MIME_TYPE, "m_foo", Fields.ATTACHMENT_CHARSET, "c_foo", Fields.ATTACHMENT_NAME, "n_foo"),

      ImmutableMultimap.of("first_name", "Nadja", Fields.ATTACHMENT_BODY, "a_bar", Fields.ATTACHMENT_MIME_TYPE, "m_bar"),

      ImmutableMultimap.of("first_name", "Nadja", Fields.ATTACHMENT_BODY, "a_baz")
      );
  }
  
  @Test
  public void testTryRulesPass() throws Exception {
    morphline = createMorphline("test-morphlines/tryRulesPass");    
    Record record = new Record();
    record.put("first_name", "Nadja");
    List<Record> expectedList = Lists.newArrayList();
    for (int i = 0; i < 2; i++) {
      Record expected = record.copy();
      expected.put("foo", "bar");
      expected.replaceValues("iter", i);
      expectedList.add(expected);
    }
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    assertEquals(expectedList, collector.getRecords());
    assertNotSame(record, collector.getRecords().get(0));
  }

  @Test
  public void testTryRulesFail() throws Exception {
    morphline = createMorphline("test-morphlines/tryRulesFail");    
    Record record = new Record();
    record.put("first_name", "Nadja");
    List<Record> expectedList = Lists.newArrayList();
    for (int i = 0; i < 2; i++) {
      Record expected = record.copy();
      expected.put("foo2", "bar2");
      expected.replaceValues("iter2", i);
      expectedList.add(expected);
    }
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    assertEquals(expectedList, collector.getRecords());
    assertNotSame(record, collector.getRecords().get(0));
  }
  
  @Test
  public void testTryRulesCatchException() throws Exception {
    morphline = createMorphline("test-morphlines/tryRulesCatchException");    
    Record record = new Record();
    record.put("first_name", "Nadja");
    List<Record> expectedList = Lists.newArrayList();
    for (int i = 0; i < 2; i++) {
      Record expected = record.copy();
      expected.put("foo2", "bar2");
      expected.replaceValues("iter2", i);
      expectedList.add(expected);
    }
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    assertEquals(expectedList, collector.getRecords());
    assertNotSame(record, collector.getRecords().get(0));
  }
  
  @Test
  public void testTryRulesFailTwice() throws Exception {
    morphline = createMorphline("test-morphlines/tryRulesFailTwice");    
    Record record = new Record();
    record.put("first_name", "Nadja");
    List<Record> expectedList = Lists.newArrayList();
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    try {
      morphline.process(record);
      fail();
    } catch (MorphlineRuntimeException e) {
      assertTrue(e.getMessage().startsWith("tryRules command found no successful rule for record"));
    }
    assertEquals(expectedList, collector.getRecords());
  }
  
  @Test
  public void testIsTrue() throws Exception {
    System.setProperty("MY_VARIABLE", "true");
    morphline = createMorphline("test-morphlines/isTrue");    
    Record record = new Record();
    record.put("isTooYoung", "true");
    processAndVerifySuccess(record, record);
    
    System.setProperty("MY_VARIABLE", "false");
    morphline = createMorphline("test-morphlines/isTrue");
    processAndVerifyFailure(createBasicRecord());
    
    System.clearProperty("MY_VARIABLE");
    try {
      morphline = createMorphline("test-morphlines/isTrue");
      fail();
    } catch (ConfigException.UnresolvedSubstitution e) {
      ; 
    }
  }
  
  @Test
  public void testIfThenElseWithThen() throws Exception {
    morphline = createMorphline("test-morphlines/ifThenElseWithThen");    
    Record record = createBasicRecord();
    processAndVerifySuccess(record, record);
    assertEquals("then1", collector.getFirstRecord().getFirstValue("state"));
  }
  
  @Test
  public void testIfThenElseWithThenEmpty() throws Exception {
    morphline = createMorphline("test-morphlines/ifThenElseWithThenEmpty");    
    Record record = createBasicRecord();
    processAndVerifySuccess(record, record);
    assertEquals("init1", collector.getFirstRecord().getFirstValue("state"));
  }
  
  @Test
  public void testIfThenElseWithElse() throws Exception {
    morphline = createMorphline("test-morphlines/ifThenElseWithElse");    
    Record record = createBasicRecord();
    processAndVerifySuccess(record, record);
    assertEquals("else1", collector.getFirstRecord().getFirstValue("state"));
  }
  
  @Test
  public void testIfThenElseWithElseEmpty() throws Exception {
    morphline = createMorphline("test-morphlines/ifThenElseWithElseEmpty");    
    Record record = createBasicRecord();
    processAndVerifySuccess(record, record);
    assertEquals("init1", collector.getFirstRecord().getFirstValue("state"));
  }
  
  @Test
  public void testNotWithTrue() throws Exception {
    morphline = createMorphline("test-morphlines/notWithTrue");    
    Record record = createBasicRecord();
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertFalse(morphline.process(record));
    assertEquals(Lists.newArrayList(), collector.getRecords());
  }
  
  @Test
  public void testNotWithFalse() throws Exception {
    morphline = createMorphline("test-morphlines/notWithFalse");    
    Record record = createBasicRecord();
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    assertEquals(record, collector.getFirstRecord());
    assertSame(record, collector.getFirstRecord());
  }

  @Test
  public void testNotWithFalseWithContinuation() throws Exception {
    morphline = createMorphline("test-morphlines/notWithFalseAndContinuation");
    Record record = createBasicRecord();
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    assertEquals(record, collector.getFirstRecord());
    assertSame(record, collector.getFirstRecord());
    assertEquals("touched", collector.getFirstRecord().getFirstValue("state"));
  }

  @Test
  public void testIfThenElseWithThenAndNot() throws Exception {
    morphline = createMorphline("test-morphlines/ifThenElseWithThenAndNot");
    Record record = createBasicRecord();
    processAndVerifySuccess(record, record);
    assertEquals("then1", collector.getFirstRecord().getFirstValue("state"));
  }
  
  @Test
  public void testReadBlob() throws Exception {
    morphline = createMorphline("test-morphlines/readBlob");    
    for (int i = 0; i < 3; i++) {
      Record record = new Record();
      byte[] bytes = "foo".getBytes("UTF-8");
      record.put(Fields.ATTACHMENT_BODY, bytes);
      collector.reset();
      startSession();
      assertEquals(1, collector.getNumStartEvents());
      assertTrue(morphline.process(record));
      assertSame(record, collector.getFirstRecord());
      assertNotSame(bytes, record.getFirstValue(Fields.ATTACHMENT_BODY)); 
      assertArrayEquals(bytes, (byte[])record.getFirstValue(Fields.ATTACHMENT_BODY)); 
    }
  }
  
  @Test
  public void testReadBlobWithDestination() throws IOException {
    morphline = createMorphline("test-morphlines/readBlobWithOutputField");
    for (int i = 0; i < 3; i++) {
      Record record = new Record();
      byte[] bytes = "foo".getBytes("UTF-8");
      record.put(Fields.ATTACHMENT_BODY, bytes);
      collector.reset();
      startSession();
      assertEquals(1, collector.getNumStartEvents());
      assertTrue(morphline.process(record));
      assertSame(record, collector.getFirstRecord());
      assertSame(bytes, record.getFirstValue(Fields.ATTACHMENT_BODY)); 
      assertNotSame(bytes, record.getFirstValue("myAwesomeDestination")); 
      assertArrayEquals(bytes, (byte[])record.getFirstValue("myAwesomeDestination")); 
    }
  }
  
  @Test
  public void testReadClob() throws Exception {
    morphline = createMorphline("test-morphlines/readClob");    
    for (int i = 0; i < 3; i++) {
      Record record = new Record();
      String msg = "foo";
      record.put(Fields.ATTACHMENT_BODY, msg.getBytes("UTF-8"));
      collector.reset();
      startSession();
      assertEquals(1, collector.getNumStartEvents());
      assertTrue(morphline.process(record));
      Record expected = new Record();
      expected.put(Fields.MESSAGE, msg);
      assertEquals(expected, collector.getFirstRecord());
      assertNotSame(record, collector.getFirstRecord());
    }
  }
  
  @Test
  public void testReadClobWithDestination() throws IOException {
    morphline = createMorphline("test-morphlines/readClobDestField");
    for (int i = 0; i < 3; i++) {
      Record record = new Record();
      String msg = "foo";
      record.put(Fields.ATTACHMENT_BODY, msg.getBytes("UTF-8"));
      collector.reset();
      startSession();
      assertEquals(1, collector.getNumStartEvents());
      assertTrue(morphline.process(record));
      Record expected = new Record();
      expected.put("myAwesomeDestination", msg);
      assertEquals(expected, collector.getFirstRecord());
      assertNotSame(record, collector.getFirstRecord());
    }
  }

  @Test
  public void testReadCSV() throws Exception {
    morphline = createMorphline("test-morphlines/readCSV");    
    for (int i = 0; i < 3; i++) {
      InputStream in = new FileInputStream(new File(RESOURCES_DIR + "/test-documents/cars2.csv"));
      Record record = new Record();
      record.put(Fields.ATTACHMENT_BODY, in);
      processAndVerifySuccess(record, 
          ImmutableMultimap.of("Age", "Age", "Extras", "Extras", "Type", "Type", "column4", "Used"),
  
          ImmutableMultimap.of("Age", "2", "Extras", "GPS", "Type", "Gas, with electric", "column4", ""),
          
          ImmutableMultimap.of("Age", "10", "Extras", "Labeled \"Vintage, 1913\"", "Type", "", "column4", "yes"),
          
          ImmutableMultimap.of("Age", "100", "Extras", "Labeled \"Vintage 1913\"", "Type", "yes"),
          
          ImmutableMultimap.of("Age", "5", "Extras", "none", "Type", "This is a\nmulti, line text", "column4", "no\""),
          
          ImmutableMultimap.of("Age", "6", "Extras", "many", "Type", "multi line2", "column4", "maybe")
          );
      in.close();
    }
  }  

  @Test
  public void testReadCSVAndIgnoreTooLongRecords() throws Exception {
    morphline = createMorphline("test-morphlines/readCSVAndIgnoreTooLongRecords");    
    for (int i = 0; i < 3; i++) {
      InputStream in = new FileInputStream(new File(RESOURCES_DIR + "/test-documents/cars2.csv"));
      Record record = new Record();
      record.put(Fields.ATTACHMENT_BODY, in);
      processAndVerifySuccess(record, 
          ImmutableMultimap.of("Age", "Age", "Extras", "Extras", "Type", "Type", "column4", "Used"),
  
          ImmutableMultimap.of("Age", "2", "Extras", "GPS", "Type", "Gas, with electric", "column4", ""),
          
          ImmutableMultimap.of("Age", "100", "Extras", "Labeled \"Vintage 1913\"", "Type", "yes"),
          
          ImmutableMultimap.of("Age", "6", "Extras", "many", "Type", "multi line2", "column4", "maybe")
          );
      in.close();
    }
  }  

  @Test
  public void testReadCSVWithoutQuoting() throws Exception {
    morphline = createMorphline("test-morphlines/readCSVWithoutQuoting");    
    for (int i = 0; i < 3; i++) {
      InputStream in = new FileInputStream(new File(RESOURCES_DIR + "/test-documents/cars.csv"));
      Record record = new Record();
      record.put(Fields.ATTACHMENT_BODY, in);
      processAndVerifySuccess(record, 
          ImmutableMultimap.of("Age", "2", "Extras", "GPS", "Type", "\"Gas", "column4", " with electric\"", "column5", "\"\""),
          
          ImmutableMultimap.of("Age", "10", "Extras", "\"Labeled \"\"Vintage", "Type", " 1913\"\"\"", "column4", "", "column5", "yes"),
          
          ImmutableMultimap.of("Age", "100", "Extras", "\"Labeled \"\"Vintage 1913\"\"\"", "Type", "yes"),
          
          ImmutableMultimap.of("Age", "5", "Extras", "none", "Type", "\"This is a"),
  
          ImmutableMultimap.of("Age", "multi", "Extras", "no")
          );
      in.close();
    }
  }  
 
  @Test
  public void testReadCSVDetail() throws Exception {
    File expectedValuesFile = new File(RESOURCES_DIR + "/test-documents/csvdetails-expected-values.txt"); 
    if (!expectedValuesFile.exists()) {
      return;
    }
    morphline = createMorphline("test-morphlines/readCSVDetails");    
    InputStream in = new FileInputStream(new File(RESOURCES_DIR + "/test-documents/csvdetails.csv"));
    Record record = new Record();
    record.put(Fields.ATTACHMENT_BODY, in);
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    Iterator<Record> iter = collector.getRecords().iterator();
    
    BufferedReader expectedReader = new BufferedReader(new InputStreamReader(new FileInputStream(expectedValuesFile), "UTF-8"));
    String line;
    long recNum = 0;
    while ((line = expectedReader.readLine()) != null) {
      String[] expectedCols = line.split(":");
      if (line.endsWith(":")) {
        expectedCols = concat(expectedCols, new String[]{""});
      }
      assertTrue("cols.length: " + expectedCols.length, expectedCols.length >= 1);
      if (expectedCols[0].startsWith("#")) {
        continue;
      }
      int expectedRecNum = Integer.parseInt(expectedCols[0]);
      expectedCols = Arrays.copyOfRange(expectedCols, 1, expectedCols.length);
      Record expectedRecord = new Record();
      for (int i = 0; i < expectedCols.length; i++) {
        expectedCols[i] = expectedCols[i].replace("\\n", "\n");
        expectedCols[i] = expectedCols[i].replace("\\r", "\r");
        expectedRecord.put("column" + i, expectedCols[i]);
      }
      
      while (iter.hasNext()) {
        Record actualRecord = iter.next();
        recNum++;
        //System.out.println("recNum:" + recNum + ":" + actualRecord);
        if (recNum == expectedRecNum) {
          //System.out.println("expect="+expectedRecord);
          //System.out.println("actual="+actualRecord);
          assertEquals(expectedRecord, actualRecord);
          break;
        }
      }
    }
    assertEquals(30, recNum);
    expectedReader.close();
  }

  @Test
  public void testReadLine() throws Exception {
    String threeLines = "first\nsecond\nthird";
    byte[] in = threeLines.getBytes("UTF-8");
    morphline = createMorphline("test-morphlines/readLine"); // uses ignoreFirstLine : true
    Record record = new Record();
    record.put(Fields.ATTACHMENT_BODY, in);
    processAndVerifySuccess(record, 
        ImmutableMultimap.of(Fields.MESSAGE, "second"), 
        ImmutableMultimap.of(Fields.MESSAGE, "third")
    );
    
    // verify counters
    boolean foundCounter = false;
    for (Entry<String, Meter> entry : morphContext.getMetricRegistry().getMeters().entrySet()) {
      if (entry.getKey().equals("morphline.readLine." + Metrics.NUM_RECORDS)) {
        assertEquals(2, entry.getValue().getCount());
        foundCounter = true;
      }
    }
    assertTrue(foundCounter);
  }  
  
  @Test
  public void testReadLineWithMimeType() throws Exception {
    String threeLines = "first\nsecond\nthird";
    byte[] in = threeLines.getBytes("UTF-8");
    morphline = createMorphline("test-morphlines/readLineWithMimeType"); // uses ignoreFirstLine : true
    Record record = new Record();
    record.put(Fields.ATTACHMENT_BODY, in);
    processAndVerifySuccess(record, 
        ImmutableMultimap.of(Fields.MESSAGE, "second"), 
        ImmutableMultimap.of(Fields.MESSAGE, "third")
    );
  }  
  
  @Test
  public void testReadLineWithMimeTypeWildcard() throws Exception {
    String threeLines = "first\nsecond\nthird";
    byte[] in = threeLines.getBytes("UTF-8");
    morphline = createMorphline("test-morphlines/readLineWithMimeTypeWildcard"); // uses ignoreFirstLine : true
    Record record = new Record();
    record.put(Fields.ATTACHMENT_BODY, in);
    processAndVerifySuccess(record, 
        ImmutableMultimap.of(Fields.MESSAGE, "second"), 
        ImmutableMultimap.of(Fields.MESSAGE, "third")
    );
  }  
  
  @Test
  public void testReadLineWithMimeTypeMismatch() throws Exception {
    String threeLines = "first\nsecond\nthird";
    byte[] in = threeLines.getBytes("UTF-8");
    morphline = createMorphline("test-morphlines/readLineWithMimeTypeMismatch"); // uses ignoreFirstLine : true
    Record record = new Record();
    record.put(Fields.ATTACHMENT_BODY, in);
    processAndVerifyFailure(record);
  }  
  
  @Test
  public void testReadMultiLineWithWhatPrevious() throws Exception {
    morphline = createMorphline("test-morphlines/readMultiLine");   
    InputStream in = new FileInputStream(new File(RESOURCES_DIR + "/test-documents/multiline-stacktrace.log"));
    Record record = new Record();
    record.put(Fields.ATTACHMENT_BODY, in);
    String multiLineEvent = Files.toString(new File(RESOURCES_DIR + "/test-documents/multiline-stacktrace-expected-long-event.log"), Charsets.UTF_8);
    processAndVerifySuccess(record, 
        ImmutableMultimap.of(Fields.MESSAGE, "juil. 25, 2012 10:49:46 AM hudson.triggers.SafeTimerTask run"),
        ImmutableMultimap.of(Fields.MESSAGE, multiLineEvent),
        ImmutableMultimap.of(Fields.MESSAGE, "juil. 25, 2012 10:49:54 AM hudson.slaves.SlaveComputer tryReconnect"),
        ImmutableMultimap.of(Fields.MESSAGE, "Infos: Attempting to reconnect CentosVagrant")
    );
    in.close();
  }  

  @Test
  public void testReadMultiLineWithWhatNext() throws Exception {
    morphline = createMorphline("test-morphlines/readMultiLineWithWhatNext");   
    InputStream in = new FileInputStream(new File(RESOURCES_DIR + "/test-documents/multiline-sessions.log"));
    Record record = new Record();
    record.put(Fields.ATTACHMENT_BODY, in);
    processAndVerifySuccess(record, 
        ImmutableMultimap.of(Fields.MESSAGE, "Started GET /foo" + "\n  Foo Started GET as HTML" + "\nCompleted 401 Unauthorized in 0ms" + "\n\n"),
        ImmutableMultimap.of(Fields.MESSAGE, "Started GET /bar" + "\n  Bar as HTML" + "\nCompleted 200 OK in 339ms"),
        ImmutableMultimap.of(Fields.MESSAGE, "Started GET /baz")
    );
    in.close();
  }  

  @Test
  public void testJavaHelloWorld() throws Exception {
    morphline = createMorphline("test-morphlines/javaHelloWorld");    
    Record record = new Record();
    record.put("tags", "hello");
    Record expected = new Record();
    expected.put("tags", "hello");
    expected.put("tags", "world");
    processAndVerifySuccess(record, expected);
  }
  
  @Test
  public void testJavaRuntimeException() throws Exception {
    morphline = createMorphline("test-morphlines/javaRuntimeException");    
    Record record = new Record();
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    try {
      morphline.process(record);
      fail();
    } catch (MorphlineRuntimeException e) {
      assertTrue(e.getMessage().startsWith("Cannot execute script"));
    }
    assertEquals(Lists.newArrayList(), collector.getRecords());
  }
  
  @Test
  public void testJavaCompilationException() throws Exception {
    Config config = parse("test-morphlines/javaCompilationException");    
    try {
      createMorphline(config);
      fail();
    } catch (MorphlineCompilationException e) {
      assertTrue(e.getMessage().startsWith("Cannot compile script"));
    }
  }
  
  @Test
  public void testGenerateUUID() throws Exception {
    testGenerateUUID("");
  }

  @Test
  public void testGenerateUUIDSecure() throws Exception {
    testGenerateUUID("Secure");
  }

  private void testGenerateUUID(String suffix) throws Exception {
    morphline = createMorphline("test-morphlines/generateUUID" + suffix);    
    Record record = new Record();
    collector.reset();
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    Record actual = collector.getFirstRecord();
    assertEquals(1, actual.get("id").size());
    String uuid = (String) actual.getFirstValue("id");
    assertEquals(36, uuid.length());
  }
  
  @Test
  public void testGrokSyslogMatch() throws Exception {
    testGrokSyslogMatchInternal(false, false);
  }
  
  @Test
  public void testGrokSyslogMatchInplace() throws Exception {
    testGrokSyslogMatchInternal(true, false);
  }
  
  @Test
  public void testGrokSyslogMatchInplaceTwoExpressions() throws Exception {
    testGrokSyslogMatchInternal(true, true, "atLeastOnce");
  }
  
  private void testGrokSyslogMatchInternal(boolean inplace, boolean twoExpressions, String... numRequiredMatchesParams) throws Exception {
    if (numRequiredMatchesParams.length == 0) {
      numRequiredMatchesParams = new String[] {"atLeastOnce", "all", "once"};
    }
    for (String numRequiredMatches : numRequiredMatchesParams) {
      morphline = createMorphline(
          "test-morphlines/grokSyslogMatch" 
          + (inplace ? "Inplace" : "")
          + (twoExpressions ? "TwoExpressions" : "") 
          + "", 
          ConfigFactory.parseMap(ImmutableMap.of("numRequiredMatches", numRequiredMatches)));
      
      Record record = new Record();
      String msg = "<164>Feb  4 10:46:14 syslog sshd[607]: Server listening on 0.0.0.0 port 22.";
      record.put(Fields.MESSAGE, msg);
      String id = "myid";
      record.put(Fields.ID, id);
      collector.reset();
      startSession();
      assertEquals(1, collector.getNumStartEvents());
      assertTrue(morphline.process(record));
      Record expected = new Record();
      expected.put(Fields.MESSAGE, msg);
      expected.put(Fields.ID, id);
      expected.put("syslog_pri", "164");
      expected.put("syslog_timestamp", "Feb  4 10:46:14");
      expected.put("syslog_hostname", "syslog");
      expected.put("syslog_program", "sshd");
      expected.put("syslog_pid", "607");
      expected.put("syslog_message", "Server listening on 0.0.0.0 port 22.");
      assertEquals(expected, collector.getFirstRecord());
      if (inplace) {
        assertSame(record, collector.getFirstRecord());
      } else {
        assertNotSame(record, collector.getFirstRecord());      
      }
      
      // mismatch
      collector.reset();
      record = new Record();
      record.put(Fields.MESSAGE, "foo" + msg);
      startSession();
      assertEquals(1, collector.getNumStartEvents());
      assertFalse(morphline.process(record));
      assertEquals(Lists.newArrayList(), collector.getRecords());
      
      // double match
      collector.reset();
      record = new Record();
      record.put(Fields.MESSAGE, msg);
      record.put(Fields.MESSAGE, msg);
      record.put(Fields.ID, id);
      record.put(Fields.ID, id);
      startSession();
      assertEquals(1, collector.getNumStartEvents());
      if ("once".equals(numRequiredMatches)) {
        assertFalse(morphline.process(record));
      } else {
        assertTrue(morphline.process(record));
        Record tmp = expected.copy();
        for (Map.Entry<String, Object> entry : tmp.getFields().entries()) {
          expected.put(entry.getKey(), entry.getValue());
        }        
        assertEquals(expected, collector.getFirstRecord());
        if (inplace) {
          assertSame(record, collector.getFirstRecord());
        } else {
          assertNotSame(record, collector.getFirstRecord());      
        }
      }
    }
  }
  
  @Test
  public void testGrokFindSubstrings() throws Exception {
    testGrokFindSubstringsInternal(false, false);
  }
  
  @Test
  public void testGrokFindSubstringsInplace() throws Exception {
    testGrokFindSubstringsInternal(true, false);
  }
  
  @Test
  public void testGrokFindSubstringsInplaceTwoExpressions() throws Exception {
    testGrokFindSubstringsInternal(true, true);
  }
  
  private void testGrokFindSubstringsInternal(boolean inplace, boolean twoExpressions) throws Exception {
    // match
    morphline = createMorphline(
        "test-morphlines/grokFindSubstrings" 
        + (inplace ? "Inplace" : "")
        + (twoExpressions ? "TwoExpressions" : "") 
        + "");
    Record record = new Record();
    String msg = "hello\t\tworld\tfoo";
    record.put(Fields.MESSAGE, msg);
    String id = "myid";
    record.put(Fields.ID, id);
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    Record expected = new Record();
    expected.put(Fields.MESSAGE, msg);
    expected.put(Fields.ID, id);
    expected.put("word", "hello");
    expected.put("word", "world");
    expected.put("word", "foo");
    assertEquals(expected, collector.getFirstRecord());
    if (inplace) {
      assertSame(record, collector.getFirstRecord());
    } else {
      assertNotSame(record, collector.getFirstRecord());      
    }
    
    // mismatch
    collector.reset();
    record = new Record();
    record.put(Fields.MESSAGE, "");
    record.put(Fields.ID, id);
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertFalse(morphline.process(record));
    assertEquals(Lists.newArrayList(), collector.getRecords());
  }
  
  @Test
  public void testGrokSeparatedValues() throws Exception {
    String msg = "hello\tworld\tfoo";
    Pattern pattern = Pattern.compile("(?<word>.+?)(\\t|\\z)");
    Matcher matcher = pattern.matcher(msg);
    List<String> results = Lists.newArrayList();
    while (matcher.find()) {
      //System.out.println("match:'" + matcher.group(1) + "'");
      results.add(matcher.group(1));
    }
    assertEquals(Arrays.asList("hello", "world", "foo"), results);
  }
  
  @Test
  public void testGrokSyslogNgCisco() throws Exception {
    morphline = createMorphline("test-morphlines/grokSyslogNgCisco");
    Record record = new Record();
    String msg = "<179>Jun 10 04:42:51 www.foo.com Jun 10 2013 04:42:51 : %myproduct-3-mysubfacility-251010: " +
    		"Health probe failed for server 1.2.3.4 on port 8083, connection refused by server";
    record.put(Fields.MESSAGE, msg);
    assertTrue(morphline.process(record));
    Record expected = new Record();
    expected.put(Fields.MESSAGE, msg);
    expected.put("cisco_message_code", "%myproduct-3-mysubfacility-251010");
    expected.put("cisco_product", "myproduct");
    expected.put("cisco_level", "3");
    expected.put("cisco_subfacility", "mysubfacility");
    expected.put("cisco_message_id", "251010");
    expected.put("syslog_message", "%myproduct-3-mysubfacility-251010: Health probe failed for server 1.2.3.4 " +
    		"on port 8083, connection refused by server");
    assertEquals(expected, collector.getFirstRecord());
    assertNotSame(record, collector.getFirstRecord());      
  }
  
  public void testGrokSyslogNgCiscoWithoutSubFacility() throws Exception {
    morphline = createMorphline("test-morphlines/grokSyslogNgCisco");
    Record record = new Record();
    String msg = "<179>Jun 10 04:42:51 www.foo.com Jun 10 2013 04:42:51 : %myproduct-3-mysubfacility-251010: " +
        "Health probe failed for server 1.2.3.4 on port 8083, connection refused by server";
    record.put(Fields.MESSAGE, msg);
    assertTrue(morphline.process(record));
    Record expected = new Record();
    expected.put(Fields.MESSAGE, msg);
    expected.put("cisco_message_code", "%myproduct-3-251010");
    expected.put("cisco_product", "myproduct");
    expected.put("cisco_level", "3");
//    expected.put("cisco_subfacility", "mysubfacility");
    expected.put("cisco_message_id", "251010");
    expected.put("syslog_message", "%myproduct-3-mysubfacility-251010: Health probe failed for server 1.2.3.4 " +
        "on port 8083, connection refused by server");
    assertEquals(expected, collector.getFirstRecord());
    assertNotSame(record, collector.getFirstRecord());      
  }
  
  @Test
  public void testGrokEmail() throws Exception {
    morphline = createMorphline("test-morphlines/grokEmail");
    Record record = new Record();
    byte[] bytes = Files.toByteArray(new File(RESOURCES_DIR + "/test-documents/email.txt"));
    record.put(Fields.ATTACHMENT_BODY, bytes);
    assertTrue(morphline.process(record));
    Record expected = new Record();
    String msg = new String(bytes, "UTF-8"); //.replaceAll("(\r)?\n", "\n");
    expected.put(Fields.MESSAGE, msg);
    expected.put("message_id", "12345.6789.JavaMail.foo@bar");
    expected.put("date", "Wed, 6 Feb 2012 06:06:05 -0800");
    expected.put("from", "foo@bar.com");
    expected.put("to", "baz@bazoo.com");
    expected.put("subject", "WEDNESDAY WEATHER HEADLINES");
    expected.put("from_names", "Foo Bar <foo@bar.com>@xxx");
    expected.put("to_names", "'Weather News Distribution' <wfoo@bar.com>");    
    expected.put("text", 
        "Average 1 to 3- degrees above normal: Mid-Atlantic, Southern Plains.." +
    		"\nAverage 4 to 6-degrees above normal: Ohio Valley, Rockies, Central Plains");
    assertEquals(expected, collector.getFirstRecord());
    assertNotSame(record, collector.getFirstRecord());      
  }
  
  @Test
  public void testGrokWithEscaping() throws Exception {
    morphline = createMorphline("test-morphlines/grokWithEscaping");
    Record record = new Record();
    record.put(Fields.MESSAGE, "{SystemSourceId}=foo");
    assertTrue(morphline.process(record));
    assertSame(record, collector.getFirstRecord());  
    
    record = new Record();
    record.put(Fields.MESSAGE, "SystemSourceId}=foo"); // missing opening brace
    assertFalse(morphline.process(record));
  }
  
  @Test
  public void testGrokWithMultipleGroupsWithSameName() throws Exception {
    morphline = createMorphline("test-morphlines/grokWithMultipleGroupsWithSameName");
    Record record = new Record();
    record.put(Fields.MESSAGE, "123 456");
    assertTrue(morphline.process(record));
    
    Record expected = new Record();
    expected.put(Fields.MESSAGE, "123 456");
    expected.put("syslog_pri", "123");
    expected.put("syslog_pri", "456");
    assertEquals(expected, collector.getFirstRecord());
    assertNotSame(record, collector.getFirstRecord());      
  }
  
  @Test
  public void testConvertTimestamp() throws Exception {
    morphline = createMorphline("test-morphlines/convertTimestamp");    
    Record record = new Record();
    record.put("ts1", "2011-09-06T14:14:34.789Z"); // "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
    record.put("ts1", "2012-09-06T14:14:34"); 
    record.put("ts1", "2013-09-06");
    Record expected = new Record();
    expected.put("ts1", "2011-09-06T07:14:34.789-0700");
    expected.put("ts1", "2012-09-06T07:14:34.000-0700");
    expected.put("ts1", "2013-09-05T17:00:00.000-0700");
    processAndVerifySuccess(record, expected);
  }
  
  @Test
  public void testDecodeBase64() throws Exception {
    morphline = createMorphline("test-morphlines/decodeBase64");    
    Record record = new Record();
    record.put("data", "SGVsbG8gV29ybGQ=");
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    byte[] actual = (byte[]) collector.getFirstRecord().getFirstValue("data");
    assertArrayEquals("Hello World".getBytes(Charsets.UTF_8), actual);
    assertSame(record, collector.getFirstRecord());    
  }
  
  @Test
  public void testFindReplace() throws Exception {
    Config override = ConfigFactory.parseString("replaceFirst : false");
    morphline = createMorphline("test-morphlines/findReplace", override);    
    Record record = new Record();
    record.put("text", "hello ic world ic");
    Record expected = new Record();
    expected.put("text", "hello I see world I see");
    processAndVerifySuccess(record, expected);
  }
  
  @Test
  public void testFindReplaceWithReplaceFirst() throws Exception {
    Config override = ConfigFactory.parseString("replaceFirst : true");
    morphline = createMorphline("test-morphlines/findReplace", override);    
    Record record = new Record();
    record.put("text", "hello ic world ic");
    Record expected = new Record();
    expected.put("text", "hello I see world ic");
    processAndVerifySuccess(record, expected);
  }
  
  @Test
  public void testFindReplaceWithGrok() throws Exception {
    Config override = ConfigFactory.parseString("replaceFirst : false");
    morphline = createMorphline("test-morphlines/findReplaceWithGrok", override);    
    Record record = new Record();
    record.put("text", "hello ic world ic");
    Record expected = new Record();
    expected.put("text", "hello! ic! world! ic!");
    processAndVerifySuccess(record, expected);
  }
  
  @Test
  public void testFindReplaceWithRegex() throws Exception {
    morphline = createMorphline("test-morphlines/findReplaceWithRegex");    
    Record record = new Record();
    record.put("text", "hello ic world ic");
    Record expected = new Record();
    expected.put("text", "hello! ic! world! ic!");
    processAndVerifySuccess(record, expected);
  }
  
  @Test
  public void testFindReplaceWithGrokWithReplaceFirst() throws Exception {
    Config override = ConfigFactory.parseString("replaceFirst : true");
    morphline = createMorphline("test-morphlines/findReplaceWithGrok", override);    
    Record record = new Record();
    record.put("text", "hello ic world ic");
    Record expected = new Record();
    expected.put("text", "hello! ic world ic");
    processAndVerifySuccess(record, expected);
  }
  
  @Test
  public void testSplit() throws Exception {
    morphline = createMorphline("test-morphlines/split");    
    Record record = new Record();
    String msg = " _a ,_b_ ,c__";
    record.put(Fields.MESSAGE, msg);
    Record expected = new Record();
    expected.put(Fields.MESSAGE, msg);
    expected.put("output", "_a");
    expected.put("output", "_b_");
    expected.put("output", "c__");
    processAndVerifySuccess(record, expected);
  }
  
  @Test
  public void testSplitWithMultipleChars() throws Exception {
    morphline = createMorphline("test-morphlines/splitWithMultipleChars");    
    Record record = new Record();
    String msg = " _a ,_b_ ,c__";
    record.put(Fields.MESSAGE, msg);
    Record expected = new Record();
    expected.put(Fields.MESSAGE, msg);
    expected.put("output", " _a");
    expected.put("output", "_b_");
    expected.put("output", "c__");
    processAndVerifySuccess(record, expected);
  }
  
  @Test
  public void testSplitWithEdgeCases() throws Exception {
    morphline = createMorphline("test-morphlines/splitWithEdgeCases");    
    Record record = new Record();
    String msg = ",, _a ,_b_ ,,";
    record.put(Fields.MESSAGE, msg);
    Record expected = new Record();
    expected.put(Fields.MESSAGE, msg);
    expected.put("output", "");
    expected.put("output", "");
    expected.put("output", "_a");
    expected.put("output", "_b_");
    expected.put("output", "");
    expected.put("output", "");
    processAndVerifySuccess(record, expected);
  }
  
  @Test
  public void testSplitWithGrok() throws Exception {
    morphline = createMorphline("test-morphlines/splitWithGrok");    
    Record record = new Record();
    String msg = " _a ,_b_ ,c__";
    record.put(Fields.MESSAGE, msg);
    Record expected = new Record();
    expected.put(Fields.MESSAGE, msg);
    expected.put("output", " _a");
    expected.put("output", "_b_");
    expected.put("output", "c__");
    processAndVerifySuccess(record, expected);
  }
  
  @Test
  public void testSplitWithOutputFields() throws Exception {
    morphline = createMorphline("test-morphlines/splitWithOutputFields");    
    Record record = new Record();
    String msg = " _a ,_b_ , ,c__,d";
    record.put(Fields.MESSAGE, msg);
    Record expected = new Record();
    expected.put(Fields.MESSAGE, msg);
    expected.put("col0", "_a");
    expected.put("col2", "c__");
    processAndVerifySuccess(record, expected);
  }
  
  @Test
  @SuppressWarnings("unchecked")
  public void testSplitKeyValue() throws Exception {
    morphline = createMorphline("test-morphlines/splitKeyValue");    
    Record record = new Record();
    record.put("params", "foo=x");
    record.put("params", " foo = y ");
    record.put("params", "foo ");
    record.put("params", "fragment=z");
    Record expected = new Record();
    expected.getFields().putAll("params", record.get("params"));
    expected.put("/foo", "x");
    expected.put("/foo", "y");
    expected.put("/fragment", "z");
    processAndVerifySuccess(record, expected);
  }
  
  @Test
  @SuppressWarnings("unchecked")
  public void testSplitKeyValueWithLiteralSeparatorOfLength3() throws Exception {
    morphline = createMorphline("test-morphlines/splitKeyValueWithLiteralSeparatorOfLength3");    
    Record record = new Record();
    record.put("params", "foo=+-x");
    record.put("params", " foo =+- y ");
    record.put("params", "foo ");
    record.put("params", "fragment=+-z");
    Record expected = new Record();
    expected.getFields().putAll("params", record.get("params"));
    expected.put("/foo", "x");
    expected.put("/foo", "y");
    expected.put("/fragment", "z");
    processAndVerifySuccess(record, expected);
  }
  
  @Test
  @SuppressWarnings("unchecked")
  public void testSplitKeyValueWithRegex() throws Exception {
    morphline = createMorphline("test-morphlines/splitKeyValueWithRegex");    
    Record record = new Record();
    record.put("params", "foo=+-x");
    record.put("params", " foo =+- y ");
    record.put("params", "foo ");
    record.put("params", "fragment=+-z");
    Record expected = new Record();
    expected.getFields().putAll("params", record.get("params"));
    expected.put("/foo", "x");
    expected.put("/ foo", "y ");
    expected.put("/fragment", "z");
    processAndVerifySuccess(record, expected);
  }
  
  @Test
  public void testSplitKeyValueWithIPTables() throws Exception {
    morphline = createMorphline("test-morphlines/splitKeyValueWithIPTables");    
    Record record = new Record();
    String msg = "Feb  6 12:04:42 IN=eth1 OUT=eth0 SRC=1.2.3.4 DST=6.7.8.9 ACK DF WINDOW=0";
    record.put(Fields.ATTACHMENT_BODY, msg.getBytes("UTF-8"));
    Record expected = new Record();
    expected.put(Fields.MESSAGE, msg);
    expected.put("timestamp", "Feb  6 12:04:42");
    expected.put("IN", "eth1");
    expected.put("OUT", "eth0");
    expected.put("SRC", "1.2.3.4");
    expected.put("DST", "6.7.8.9");
    expected.put("WINDOW", "0");
    processAndVerifySuccess(record, expected, false);
  }
  
  @Test
  public void testStartReportingMetricsToCSV() throws Exception {
    File testMetricsOutput1 = new File("target/testMetricsOutput1/morphline.logDebug.numProcessCalls.csv");
    File testMetricsOutput2 = new File("target/testMetricsOutput2/morphline.logDebug.numProcessCalls.csv");
    FileUtils.deleteDirectory(testMetricsOutput1.getParentFile());
    FileUtils.deleteDirectory(testMetricsOutput2.getParentFile());
    assertFalse(testMetricsOutput1.getParentFile().exists());
    assertFalse(testMetricsOutput2.getParentFile().exists());
    morphline = createMorphline("test-morphlines/startReportingMetricsToCSV");    
    try {
      Record record = new Record();
      String msg = "foo";
      record.put(Fields.MESSAGE, msg);
      Record expected = new Record();
      expected.put(Fields.MESSAGE, msg);
      processAndVerifySuccess(record, expected);
      
      // verify reporter is active, i.e. verify reporter is writing to file
      waitForFileLengthGreaterThan(testMetricsOutput1, 0);
      waitForFileLengthGreaterThan(testMetricsOutput2, 0);
      assertTrue(testMetricsOutput1.isFile());
      assertTrue(testMetricsOutput2.isFile());
      long len1 = testMetricsOutput1.length();
      long len2 = testMetricsOutput2.length();      
      assertTrue(len1 > 0);
      assertTrue(len2 > 0);
      for (int i = 0; i < 2; i++) {
        waitForFileLengthGreaterThan(testMetricsOutput1, len1);
        waitForFileLengthGreaterThan(testMetricsOutput2, len2);
        long len1b = testMetricsOutput1.length();
        long len2b = testMetricsOutput2.length();      
        assertTrue(len1b > len1);
        assertTrue(len2b > len2);
        len1 = len1b;
        len2 = len2b;
      }
      Notifications.notifyShutdown(morphline);
      
      // verify reporter is shutdown, i.e. verify reporter isn't writing to file anymore
      len1 = testMetricsOutput1.length();
      len2 = testMetricsOutput2.length();      
      for (int i = 0; i < 2; i++) { 
        Thread.sleep(200);
        assertEquals(len1, testMetricsOutput1.length());
        assertEquals(len2, testMetricsOutput2.length());
        Notifications.notifyShutdown(morphline);
      }
    } finally {
      Notifications.notifyShutdown(morphline);
    }
  }

  private void waitForFileLengthGreaterThan(File file, long minLength) throws InterruptedException {
    long timeout = System.currentTimeMillis() + 1000;
    while (file.length() <= minLength && System.currentTimeMillis() <= timeout) {
      Thread.sleep(10);
    }
  }

  @Test
  public void testStartReportingMetricsToJMX() throws Exception {
    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    ObjectName obj1Name = new ObjectName("domain1", "name", "morphline.logDebug.numProcessCalls");
    ObjectName obj2Name = new ObjectName("domain2", "name", "morphline.logDebug.numProcessCalls");
    ObjectName timerName = new ObjectName("domain1", "name", "myMetrics.myTimer");
    ObjectName timer2Name = new ObjectName("domain1", "name", "myMetrics.myTimer2");
    assertMBeanInstanceNotFound(obj1Name, mBeanServer);
    assertMBeanInstanceNotFound(obj2Name, mBeanServer);
    assertMBeanInstanceNotFound(timerName, mBeanServer);
    assertMBeanInstanceNotFound(timer2Name, mBeanServer);
    morphline = createMorphline("test-morphlines/startReportingMetricsToJMX");    
    
    assertEquals(0L, mBeanServer.getAttribute(obj1Name, "Count"));
    mBeanServer.getMBeanInfo(obj1Name);
    assertEquals(0L, mBeanServer.getAttribute(obj2Name, "Count"));
    mBeanServer.getMBeanInfo(obj2Name);
    assertMBeanInstanceNotFound(timerName, mBeanServer);
    assertMBeanInstanceNotFound(timer2Name, mBeanServer);
    
    Record record = new Record();
    String msg = "foo";
    record.put(Fields.MESSAGE, msg);
    Record expected = new Record();
    expected.put(Fields.MESSAGE, msg);
    processAndVerifySuccess(record, expected);
    
    // verify reporter is active
    assertEquals(2L, mBeanServer.getAttribute(obj1Name, "Count"));
    assertEquals("events/second", mBeanServer.getAttribute(obj1Name, "RateUnit"));
    mBeanServer.getMBeanInfo(obj1Name);
    assertEquals(2L, mBeanServer.getAttribute(obj2Name, "Count"));
    assertEquals("events/second", mBeanServer.getAttribute(obj2Name, "RateUnit"));
    mBeanServer.getMBeanInfo(obj2Name);    
    assertEquals(1L, mBeanServer.getAttribute(timerName, "Count"));
    assertEquals("milliseconds", mBeanServer.getAttribute(timerName, "DurationUnit"));
    assertEquals("events/millisecond", mBeanServer.getAttribute(timerName, "RateUnit"));
    mBeanServer.getMBeanInfo(timerName);    
    assertEquals(1L, mBeanServer.getAttribute(timer2Name, "Count"));
    assertEquals("milliseconds", mBeanServer.getAttribute(timer2Name, "DurationUnit"));
    assertEquals("events/minute", mBeanServer.getAttribute(timer2Name, "RateUnit"));
    mBeanServer.getMBeanInfo(timerName);    
    
    // verify reporter is shutdown
    for (int i = 0; i < 2; i++) {
      Notifications.notifyShutdown(morphline);
      assertMBeanInstanceNotFound(obj1Name, mBeanServer);
      assertMBeanInstanceNotFound(obj2Name, mBeanServer);
      assertMBeanInstanceNotFound(timerName, mBeanServer);
      assertMBeanInstanceNotFound(timer2Name, mBeanServer);
    }
  }
  
  @Test
  public void testStartReportingMetricsToSLF4J() throws Exception {
    morphline = createMorphline("test-morphlines/startReportingMetricsToSLF4J");    
    Record record = new Record();
    String msg = "foo";
    record.put(Fields.MESSAGE, msg);
    Record expected = new Record();
    expected.put(Fields.MESSAGE, msg);
    processAndVerifySuccess(record, expected);
    Notifications.notifyShutdown(morphline);
    Notifications.notifyShutdown(morphline);
  }

  private void assertMBeanInstanceNotFound(ObjectName objName, MBeanServer mBeanServer) 
      throws IntrospectionException, ReflectionException, AttributeNotFoundException, MBeanException {
    
    try {
      mBeanServer.getMBeanInfo(objName);
      fail();
    } catch (InstanceNotFoundException e) {
      ; // expected
    }
    
    try {
      mBeanServer.getAttribute(objName, "Count");
      fail();
    } catch (InstanceNotFoundException e) {
      ; // expected
    }
  }
  
  @Test
  public void testTranslate() throws Exception {
    morphline = createMorphline("test-morphlines/translate");    
    Record record = new Record();
    Record expected = new Record();
    
    record.replaceValues("level", "0");
    expected.replaceValues("level", "Emergency");
    processAndVerifySuccess(record, expected);
    
    record.replaceValues("level", 0);
    expected.replaceValues("level", "Emergency");
    processAndVerifySuccess(record, expected);
    
    record.replaceValues("level", "1");
    expected.replaceValues("level", "Alert");
    processAndVerifySuccess(record, expected);
    
    record.replaceValues("level", 1);
    expected.replaceValues("level", "Alert");
    processAndVerifySuccess(record, expected);
    
    record.replaceValues("level", 999);
    expected.replaceValues("level", "unknown");
    processAndVerifySuccess(record, expected);
  }
    
  @Test
  public void testTranslateFailure() throws Exception {
    morphline = createMorphline("test-morphlines/translateFailure");    
    Record record = new Record();
    record.replaceValues("level", 999);
    processAndVerifyFailure(record);
  }

  @Test
  public void testConvertTimestampEmpty() throws Exception {
    morphline = createMorphline("test-morphlines/convertTimestamp");
    Record record = new Record();
    Record expected = new Record();
    processAndVerifySuccess(record, expected);
  }
  
  @Test
  public void testConvertTimestampBad() throws Exception {
    morphline = createMorphline("test-morphlines/convertTimestamp");
    Record record = new Record();
    record.put("ts1", "this is an invalid timestamp");
    processAndVerifyFailure(record);
  }
  
  @Test
  public void testConvertTimestampWithDefaults() throws Exception {
    morphline = createMorphline("test-morphlines/convertTimestampWithDefaults");    
    Record record = new Record();
    record.put(Fields.TIMESTAMP, "2011-09-06T14:14:34.789Z");
    record.put(Fields.TIMESTAMP, "2012-09-06T14:14:34"); 
    record.put(Fields.TIMESTAMP, "2013-09-06");
    record.put(Fields.TIMESTAMP, "Wednesday, 31-Dec-14 08:00:00 +0600");    
    Record expected = new Record();
    expected.put(Fields.TIMESTAMP, "2011-09-06T14:14:34.789Z");
    expected.put(Fields.TIMESTAMP, "2012-09-06T14:14:34.000Z");
    expected.put(Fields.TIMESTAMP, "2013-09-06T00:00:00.000Z");
    expected.put(Fields.TIMESTAMP, "2014-12-31T02:00:00.000Z");
    processAndVerifySuccess(record, expected);
  }
  
  @Test
  public void testConvertTimestampWithBadTimezone() throws Exception {
    Config config = parse("test-morphlines/convertTimestampWithBadTimezone");    
    try {
      createMorphline(config);
      fail();
    } catch (MorphlineCompilationException e) {
      assertTrue(e.getMessage().startsWith("Unknown timezone"));
    }
  }
  
  @Test
  public void testConvertTimestampWithInputFormatUnixTimeInMillis() throws Exception {
    morphline = createMorphline("test-morphlines/convertTimestampWithInputFormatUnixTimeInMillis");    
    Record record = new Record();
    record.put("ts1", "0");
    record.put("ts1", "1370636123501"); 
    Record expected = new Record();
    expected.put("ts1", "1970-01-01T00:00:00.000Z");
    expected.put("ts1", "2013-06-07T20:15:23.501Z");
    processAndVerifySuccess(record, expected);
  }
  
  @Test
  public void testConvertTimestampWithInputFormatUnixTimeInSeconds() throws Exception {
    morphline = createMorphline("test-morphlines/convertTimestampWithInputFormatUnixTimeInSeconds");    
    Record record = new Record();
    record.put("ts1", "0");
    record.put("ts1", "1370636123");
    Record expected = new Record();
    expected.put("ts1", "1970-01-01T00:00:00.000Z");
    expected.put("ts1", "2013-06-07T20:15:23.000Z");
    processAndVerifySuccess(record, expected);
  }
  
  @Test
  public void testConvertTimestampWithOutputFormatUnixTimeInMillis() throws Exception {
    morphline = createMorphline("test-morphlines/convertTimestampWithOutputFormatUnixTimeInMillis");    
    Record record = new Record();
    record.put("ts1", "1970-01-01T00:00:00.000Z");
    record.put("ts1", "2013-06-07T20:15:23.501Z");
    Record expected = new Record();
    expected.put("ts1", "0");
    expected.put("ts1", "1370636123501"); 
    processAndVerifySuccess(record, expected);
  }
  
  @Test
  public void testConvertTimestampWithOutputFormatUnixTimeInSeconds() throws Exception {
    morphline = createMorphline("test-morphlines/convertTimestampWithOutputFormatUnixTimeInSeconds");    
    Record record = new Record();
    record.put("ts1", "1970-01-01T00:00:00.000Z");
    record.put("ts1", "2013-06-07T20:15:23.501Z");
    Record expected = new Record();
    expected.put("ts1", "0");
    expected.put("ts1", "1370636123"); 
    processAndVerifySuccess(record, expected);
  }
  
  @Test
  public void testExtractURIComponents() throws Exception {
    String uriStr = "http://user-info@www.fool.com:8080/errors.log?foo=x&foo=y&foo=z#fragment";
    morphline = createMorphline("test-morphlines/extractURIComponents");    
    Record record = new Record();
    record.put("uri", uriStr);
    
    Record expected = new Record();
    URI uri = new URI(uriStr);
    String prefix = "uri_component_";
    expected.put("uri", uriStr);
    expected.put(prefix + "scheme", uri.getScheme());
    expected.put(prefix + "authority", uri.getAuthority());
    expected.put(prefix + "path", uri.getPath());
    expected.put(prefix + "query", uri.getQuery());
    expected.put(prefix + "fragment", uri.getFragment());
    expected.put(prefix + "host", uri.getHost());
    expected.put(prefix + "port", uri.getPort());
    expected.put(prefix + "schemeSpecificPart", uri.getSchemeSpecificPart());
    expected.put(prefix + "userInfo", uri.getUserInfo());
    
    processAndVerifySuccess(record, expected);
    
    record = new Record();
    record.put("uri", "invalidURI:");
    processAndVerifyFailure(record);
  }
  
  @Test
  public void testExtractURIComponent() throws Exception {
    String uriStr = "http://user-info@www.fool.com:8080/errors.log?foo=x&foo=y&foo=z#fragment";
    URI uri = new URI(uriStr);
    testExtractURIComponent2(uriStr, "scheme", uri.getScheme());
    testExtractURIComponent2(uriStr, "authority", uri.getAuthority());
    testExtractURIComponent2(uriStr, "path", uri.getPath());
    testExtractURIComponent2(uriStr, "query", uri.getQuery());
    testExtractURIComponent2(uriStr, "fragment", uri.getFragment());
    testExtractURIComponent2(uriStr, "host", uri.getHost());
    testExtractURIComponent2(uriStr, "port", uri.getPort());
    testExtractURIComponent2(uriStr, "schemeSpecificPart", uri.getSchemeSpecificPart());
    testExtractURIComponent2(uriStr, "userInfo", uri.getUserInfo());
    try {
      testExtractURIComponent2(uriStr, "illegalType", uri.getUserInfo());
      fail();
    } catch (MorphlineCompilationException e) {
      ; // expected
    }
    testExtractURIComponent2("invalidURI:", "host", uri.getHost(), false);
  }
  
  private void testExtractURIComponent2(String uriStr, String component, Object expectedComponent) throws Exception {
    testExtractURIComponent2(uriStr, component, expectedComponent, true); 
  }
  
  private void testExtractURIComponent2(String uriStr, String component, Object expectedComponent, boolean success) throws Exception {
    morphline = createMorphline(
        "test-morphlines/extractURIComponent", 
        ConfigFactory.parseMap(ImmutableMap.of("component", component)));
    
    Record record = new Record();
    record.put("uri", uriStr);        
    Record expected = new Record();
    expected.put("uri", uriStr);
    expected.put("output", expectedComponent);
    if (success) {
      processAndVerifySuccess(record, expected);
    } else {
      processAndVerifyFailure(record);      
    }
  }
  
  @Test
  public void testExtractURIQueryParameters() throws Exception {
    String host = "http://www.fool.com/errors.log";
    internalExtractURIQueryParams("foo", host + "?foo=x&foo=y&foo=z", Arrays.asList("x", "y", "z"));
    internalExtractURIQueryParams("foo", host + "?foo=x&foo=y&foo=z#fragment", Arrays.asList("x", "y", "z"));
    internalExtractURIQueryParams("foo", host + "?boo=x&foo=y&boo=z", Arrays.asList("y"));
    internalExtractURIQueryParams("foo", host + "?boo=x&bar=y&baz=z", Lists.newArrayList());

    internalExtractURIQueryParams("foo", host + "?foo=x&foo=y&foo=z", Arrays.asList("x"), 1);
    internalExtractURIQueryParams("foo", host + "?foo=x&foo=y&foo=z", Lists.newArrayList(), 0);

    internalExtractURIQueryParams("foo", "", Lists.newArrayList());
    internalExtractURIQueryParams("foo", "?", Lists.newArrayList());
    internalExtractURIQueryParams("foo", "::", Lists.newArrayList()); // syntax error in URI
    internalExtractURIQueryParams("foo", new String(new byte[10], "ASCII"), Lists.newArrayList());
    internalExtractURIQueryParams("foo", host + "", Lists.newArrayList());
    internalExtractURIQueryParams("foo", host + "?", Lists.newArrayList());
    
    internalExtractURIQueryParams("foo", host + "?foo=hello%26%3D%23&bar=world", Arrays.asList("hello&=#"));
    internalExtractURIQueryParams("foo&=#", host + "?foo%26%3D%23=hello%26%3D%23&bar=world", Arrays.asList("hello&=#"));
    internalExtractURIQueryParams("foo&=#", host + "?foo&=#=hello%26%3D%23&bar=world", Lists.newArrayList());
    internalExtractURIQueryParams("foo%26%3D%23", host + "?foo%26%3D%23=hello%26%3D%23&bar=world", Lists.newArrayList());
    
    internalExtractURIQueryParams("bar", host + "?foo=hello%26%3D%23&bar=world", Arrays.asList("world"));
    internalExtractURIQueryParams("bar", host + "?foo%26%3D%23=hello%26%3D%23&bar=world", Arrays.asList("world"));
    internalExtractURIQueryParams("bar", host + "?foo&===hello%26%3D%23&bar=world", Arrays.asList("world"));
  }
  
  private void internalExtractURIQueryParams(String paramName, String url, List expected) throws Exception {
    internalExtractURIQueryParams(paramName, url, expected, -1);
  }
  
  @SuppressWarnings("unchecked")
  private void internalExtractURIQueryParams(String paramName, String url, List expected, int maxParams) throws Exception {
    String fileName = "test-morphlines/extractURIQueryParameters";
    String overridesStr = "queryParam : " + ConfigUtil.quoteString(paramName);
    if (maxParams >= 0) {
      fileName += "WithMaxParameters";
      overridesStr += "\nmaxParameters : " + maxParams;
    }
    Config override = ConfigFactory.parseString(overridesStr);
    morphline = createMorphline(fileName, override);
    Record record = new Record();
    record.put("in", url);
    Record expectedRecord = new Record();
    expectedRecord.put("in", url);
    expectedRecord.getFields().putAll("out", expected);
    processAndVerifySuccess(record, expectedRecord);
  }
  
  @Test
  public void testImportSpecs() {
    List<String> importSpecs = Arrays.asList("org.kitesdk.**", "org.apache.solr.**", "net.*", getClass().getName());
    for (Class clazz : new MorphlineContext().getTopLevelClasses(importSpecs, CommandBuilder.class)) {
      //System.out.println("found " + clazz);
    }
    MorphlineContext ctx = new MorphlineContext.Builder().build(); 
    ctx.importCommandBuilders(importSpecs);
    ctx.importCommandBuilders(importSpecs);    
  }
  
  @Test
  public void testImportSpecsWithOnlyFQCNs() {
    List<String> importSpecs = Arrays.asList(getClass().getName());
    for (Class clazz : new MorphlineContext().getTopLevelClasses(importSpecs, CommandBuilder.class)) {
      //System.out.println("found " + clazz);
    }
    MorphlineContext ctx = new MorphlineContext.Builder().build(); 
    ctx.importCommandBuilders(importSpecs);
    ctx.importCommandBuilders(importSpecs);    
  }
  
  @Test
  @Ignore
  public void testHugeImportSpecs() {
    long start = System.currentTimeMillis();
    List<String> importSpecs = Arrays.asList("com.**", "org.**", "net.*", getClass().getName());
    for (Class clazz : new MorphlineContext().getTopLevelClasses(importSpecs, CommandBuilder.class)) {
      System.out.println("found " + clazz);
    }
    float secs = (System.currentTimeMillis() - start) / 1000.0f;
    System.out.println("secs=" + secs);
  }
  
  @Test
  @Ignore
  public void testFindResources() throws Exception {    
    // TODO maybe expose as Resources.copyClassPathFilesToCWD("test-morphlines/");
    // or importClassPathFiles(...)
    for (ResourceInfo info : ClassPath.from(getClass().getClassLoader()).getResources()) {
      if (info.getResourceName().startsWith("test-morphlines/")) {
        System.out.println("info=" + info.url());        
//        ByteStreams.toByteArray(info.url().openStream());
      }
    }
    //  Enumeration<URL> iter = getClass().getClassLoader().getResources("test-morphlines");
    //  while (iter.hasMoreElements()) {
    //    URL url = iter.nextElement();
    //    System.out.println("url=" + url);
    //  }
  }
  
  private Record createBasicRecord() {
    Record record = new Record();
    record.put("first_name", "Nadja");
    record.put("age", 8);
    record.put("tags", "one");
    record.put("tags", 2);
    record.put("tags", "three");
    return record;
  }

  @Test
  @Ignore
  // Before running this disable debug logging 
  // via log4j.logger.org.kitesdk.morphline=INFO in log4j.properties
  public void benchmark() throws Exception {
    String morphlineConfigFile = "test-morphlines/readCSVWithoutQuoting";
    //String morphlineConfigFile = "test-morphlines/readCSVWithoutQuotingUsingSplit";
    //String morphlineConfigFile = "test-morphlines/grokEmail";
    //String morphlineConfigFile = "test-morphlines/grokSyslogNgCisco";
    long durationSecs = 20;
    //File file = new File(RESOURCES_DIR + "/test-documents/email.txt");
    //File file = new File(RESOURCES_DIR + "/test-documents/emails.txt");
    File file = new File(RESOURCES_DIR + "/test-documents/cars3.csv");
    String msg = "<179>Jun 10 04:42:51 www.foo.com Jun 10 2013 04:42:51 : %myproduct-3-mysubfacility-251010: " +
        "Health probe failed for server 1.2.3.4 on port 8083, connection refused by server";
    System.out.println("Now benchmarking " + morphlineConfigFile + " ...");
    morphline = createMorphline(morphlineConfigFile);    
    byte[] bytes = Files.toByteArray(file);
    long start = System.currentTimeMillis();
    long duration = durationSecs * 1000;
    int iters = 0; 
    while (System.currentTimeMillis() < start + duration) {
      Record record = new Record();
      record.put(Fields.ATTACHMENT_BODY, bytes);      
//    record.put(Fields.MESSAGE, msg);      
      collector.reset();
      startSession();
      assertEquals(1, collector.getNumStartEvents());
      assertTrue(morphline.process(record));    
      iters++;
    }
    float secs = (System.currentTimeMillis() - start) / 1000.0f;
    System.out.println("Results: iters=" + iters + ", took[secs]=" + secs + ", iters/secs=" + (iters/secs));
  }  

}
