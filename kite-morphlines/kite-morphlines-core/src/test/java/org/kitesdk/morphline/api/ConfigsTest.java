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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.morphline.base.Configs;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;


public class ConfigsTest extends Assert {
  
  @Test
  public void testBasic() throws Exception {
    assertTimeUnitEquals(TimeUnit.DAYS, "days");
    assertTimeUnitEquals(TimeUnit.DAYS, "day");
    assertTimeUnitEquals(TimeUnit.DAYS, "d");
    assertTimeUnitEquals(TimeUnit.HOURS, "hours");
    assertTimeUnitEquals(TimeUnit.HOURS, "hour");
    assertTimeUnitEquals(TimeUnit.HOURS, "h");
    assertTimeUnitEquals(TimeUnit.MINUTES, "minutes");
    assertTimeUnitEquals(TimeUnit.MINUTES, "minute");
    assertTimeUnitEquals(TimeUnit.MINUTES, "m");
    assertTimeUnitEquals(TimeUnit.SECONDS, "seconds");
    assertTimeUnitEquals(TimeUnit.SECONDS, "second");
    assertTimeUnitEquals(TimeUnit.SECONDS, "s");
    assertTimeUnitEquals(TimeUnit.MILLISECONDS, "milliseconds");
    assertTimeUnitEquals(TimeUnit.MILLISECONDS, "millisecond");
    assertTimeUnitEquals(TimeUnit.MILLISECONDS, "ms");
    assertTimeUnitEquals(TimeUnit.MICROSECONDS, "microseconds");
    assertTimeUnitEquals(TimeUnit.MICROSECONDS, "microsecond");
    assertTimeUnitEquals(TimeUnit.MICROSECONDS, "us");    
    assertTimeUnitEquals(TimeUnit.NANOSECONDS, "nanoseconds");
    assertTimeUnitEquals(TimeUnit.NANOSECONDS, "nanosecond");
    assertTimeUnitEquals(TimeUnit.NANOSECONDS, "ns");
  }
  
  private void assertTimeUnitEquals(TimeUnit unit, String str) {
    Config config = ConfigFactory.parseString("foo : " + str);
    assertSame(unit, new Configs().getTimeUnit(config, "foo"));    
  }
  
  @Test
  public void testEmptyStringThrowsSyntaxError() throws Exception {
    Config config = ConfigFactory.parseString("foo : \"\"");
    try {
      new Configs().getTimeUnit(config, "foo");
      fail();
    } catch (IllegalArgumentException e) {
      ; // expected
    }
  }
  
  @Test
  public void testValidateArgumentsWithoutQuotes() throws Exception {
    Config config = ConfigFactory.parseString("{ foo : bar, see : you.there.soon }");
    Configs configs = new Configs();
    assertEquals("bar", configs.getString(config, "foo"));
    assertEquals("you.there.soon", configs.getString(config, "see"));
    configs.validateArguments(config);    
  }
  
  @Test
  public void testValidateArgumentsWithQuotes() throws Exception {
    Config config = ConfigFactory.parseString("{ foo : bar, \"see\" : \"you.there.soon.~!@#$%^&*()_+=-\" }");
    Configs configs = new Configs();
    assertEquals("bar", configs.getString(config, "foo"));
    assertEquals("you.there.soon.~!@#$%^&*()_+=-", configs.getString(config, "see"));
    configs.validateArguments(config);    
  }
  
  @Test
  public void testQuotingInHashes() throws Exception {
    String q = String.valueOf('"');
    testQuotingInHashesInternal("foo", "bar");
    testQuotingInHashesInternal(q + "foo" + q, "bar");
    testQuotingInHashesInternal(q + "foo" + q, q + "bar" + q);
    testQuotingInHashesInternal(q + "" + q, q + "" + q);
    testQuotingInHashesInternal("foo.bar.baz", "get.there.soon");
    testQuotingInHashesInternal(q + "foo.bar.baz" + q, "get.there.soon");
    testQuotingInHashesInternal(q + "foo.bar.baz" + q, q + "get.there.soon" + q);
    testQuotingInHashesInternal(q + "foo~!@#$%^&*()_+=-" + q, q + "bar~!@#$%^&*()_+=-" + q);
    testQuotingInHashesInternal(q + "foo.~!@#$%^&*()_+=-" + q, q + "bar.~!@#$%^&*()_+=-" + q);
  }
  
  private void testQuotingInHashesInternal(String key, String value) throws Exception {
    Config config = ConfigFactory.parseString("{ " + key + " : " + value + "}");
    assertNameValueEquals(trimQuote(key), trimQuote(value), config);
    assertNameValueEquals2(trimQuote(key), trimQuote(value), config);
  }
  
  private void assertNameValueEquals(String key, String value, Config config) {
    for (Map.Entry<String, Object> entry : new Configs().getEntrySet(config)) {
      assertEquals(key, entry.getKey());
      assertEquals(value, entry.getValue());
    }
  }

  private void assertNameValueEquals2(String key, String value, Config config) {
    for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
      //assertEquals(key, entry.getKey()); // would fail
      assertEquals(key, trimQuote(entry.getKey()));
      String actualValue = entry.getValue().unwrapped().toString();
      assertEquals(value, actualValue);
      actualValue = config.getString(entry.getKey());
      assertEquals(value, actualValue);
    }
  }
  
  private String trimQuote(String str) {
    if (str.length() > 1 && str.startsWith("\"") && str.endsWith("\"")) {
      return str.substring(1, str.length() - 1);
    } else {
      return str;
    }
  }
  
}
