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
package org.kitesdk.morphline.stdlib;

import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.base.Configs;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Timer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


public class PatternMetricFilterTest extends Assert {
  
  @Test
  public void testCompletelyEmpty() throws Exception {
    String str = "{}";
    Config config = ConfigFactory.parseString(str);
    MetricFilter filter = PatternMetricFilter.parse(new Configs(), config);
    assertSame(filter, MetricFilter.ALL);
    assertTrue(filter.matches("foo", new Counter()));
  }
  
  @Test
  public void testEmpty() throws Exception {
    String str = "{ metricFilter : {} }";
    Config config = ConfigFactory.parseString(str);
    MetricFilter filter = PatternMetricFilter.parse(new Configs(), config);
    assertNotSame(filter, MetricFilter.ALL);
    assertTrue(filter.matches("foo", new Counter()));
    assertTrue(filter.toString().length() > 0);
  }
  
  @Test
  public void testComplex() throws Exception {
    String str = "{" + 
      "\n metricFilter : {" +
      "\n includes : { " +
      "\n \"literal:foo\" : \"glob:foo*\"" +
      "\n \"regex:.*\" : \"glob:*\"" +
      "\n \"regex:.*.*\" : \"glob:**\"" +              
      "\n }" +
      "\n excludes : { # if missing defaults to match none" +
      "\n \"literal:foo.bar\" : \"glob:*\"" +
      "\n \"literal:boo\" : \"glob:*Timer\"" +
      "\n }}}";

    Config config = ConfigFactory.parseString(str);
    MetricFilter filter = PatternMetricFilter.parse(new Configs(), config);
    assertNotSame(filter, MetricFilter.ALL);
    assertTrue(filter.matches("foo", new Counter()));
    assertTrue(filter.matches("boo", new Counter()));
    assertFalse(filter.matches("foo.bar", new Counter()));
    assertFalse(filter.matches("boo", new Timer()));
    assertTrue(filter.toString().length() > 0);
  }
  
  @Test
  public void testNotIncluded() throws Exception {
    String str = "{" + 
      "\n metricFilter : {" +
      "\n includes : { " +
      "\n \"literal:foo\" : \"glob:*\"" +
      "\n }" +
      "\n }}";

    Config config = ConfigFactory.parseString(str);
    MetricFilter filter = PatternMetricFilter.parse(new Configs(), config);
    assertNotSame(filter, MetricFilter.ALL);
    assertTrue(filter.matches("foo", new Counter()));
    assertFalse(filter.matches("boo", new Counter()));
    assertTrue(filter.toString().length() > 0);
  }
  
  @Test
  public void testWildcardMeansMatchAll() throws Exception {
    String str = "{" + 
      "\n metricFilter : {" +
      "\n includes : { " +
      "\n \"*\" : \"*\"" +
      "\n }" +
      "\n }}";

    Config config = ConfigFactory.parseString(str);
    MetricFilter filter = PatternMetricFilter.parse(new Configs(), config);
    assertNotSame(filter, MetricFilter.ALL);
    assertTrue(filter.matches("foo", new Counter()));
    assertTrue(filter.matches("boo", new Counter()));
    assertTrue(filter.toString().length() > 0);
  }
  
  @Test
  public void testUnrecognizedPatternType() throws Exception {
    String str = "{" + 
      "\n metricFilter : {" +
      "\n includes : { " +
      "\n \"unRecognizedType:foo\" : \"glob:*\"" +
      "\n }" +
      "\n }}";

    Config config = ConfigFactory.parseString(str);
    try {
      PatternMetricFilter.parse(new Configs(), config);
      fail();
    } catch (MorphlineCompilationException e) {
      ; // expected
    }
  }
  
  @Test
  public void testIllegalPatternType() throws Exception {
    String str = "{" + 
      "\n metricFilter : {" +
      "\n includes : { " +
      "\n \"ILLEGAL\" : \"glob:*\"" +
      "\n }" +
      "\n }}";

    Config config = ConfigFactory.parseString(str);
    try {
      PatternMetricFilter.parse(new Configs(), config);
      fail();
    } catch (MorphlineCompilationException e) {
      ; // expected
    }
  }
  
}
