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
import org.kitesdk.morphline.base.Configs;
import org.kitesdk.morphline.shaded.com.google.code.regexp.Pattern;

import com.typesafe.config.ConfigFactory;

public class GrokDictionaryTest extends Assert {

  @Test
  public void testGrokISO8601() {
    String str = "{ dictionaryFiles : [target/test-classes/grok-dictionaries/grok-patterns] }";
    GrokDictionaries dicts = new GrokDictionaries(ConfigFactory.parseString(str), new Configs());
    Pattern pattern = dicts.compileExpression("%{TIMESTAMP_ISO8601:timestamp}");
    assertTrue(pattern.matcher("2007-03-01T13:00:00").matches());
    assertTrue(pattern.matcher("2007-03-01T13:00:00Z").matches());
    assertTrue(pattern.matcher("2007-03-01T13:00:00+01:00").matches());
    assertTrue(pattern.matcher("2007-03-01T13:00:00+0100").matches());
    assertTrue(pattern.matcher("2007-03-01T13:00:00+01").matches());
    assertFalse(pattern.matcher("2007-03-01T13:00:00Z+01:00").matches());
  }

  @Test
  public void testResourceLoad() {
    String str = "{ dictionaryResources : [grok-dictionaries/grok-patterns] }";
    GrokDictionaries dicts = new GrokDictionaries(ConfigFactory.parseString(str), new Configs());
    Pattern pattern = dicts.compileExpression("%{TIMESTAMP_ISO8601:timestamp}");
    assertTrue(pattern.matcher("2007-03-01T13:00:00").matches());
    assertTrue(pattern.matcher("2007-03-01T13:00:00Z").matches());
    assertTrue(pattern.matcher("2007-03-01T13:00:00+01:00").matches());
    assertTrue(pattern.matcher("2007-03-01T13:00:00+0100").matches());
    assertTrue(pattern.matcher("2007-03-01T13:00:00+01").matches());
    assertFalse(pattern.matcher("2007-03-01T13:00:00Z+01:00").matches());
  }

}
