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

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TypedSettingsTest extends Assert {
  
  private Map<String, Object> map;
  private TypedSettings settings;
  
  @Before 
  public void setUp() {
    map = new HashMap<String, Object>();
    settings = new MorphlineContext.Builder().setSettings(map).build().getTypedSettings();
  }
  
  @Test
  public void testObject() throws Exception {
    String key = "myKey";
    assertEquals("true", settings.getObject(key, "true"));
    assertEquals("blue", settings.getObject(key, "blue"));
    map.put(key, "red");
    assertEquals("red", settings.getObject(key, "blue"));
    map.put(key, new Object());
    settings.getObject(key, "foo");
  }
  
  @Test
  public void testString() throws Exception {
    String key = "myKey";
    assertEquals("true", settings.getString(key, "true"));
    assertEquals("blue", settings.getString(key, "blue"));
    map.put(key, "red");
    assertEquals("red", settings.getString(key, "blue"));
    map.put(key, new Object());
    try {
      settings.getString(key, "foo");
      fail();
    } catch (ClassCastException e) {
      ; // to be expected
    }    
  }
  
  @Test
  public void testBoolean() throws Exception {
    String key = "myKey";
    assertTrue(settings.getBoolean(key, true));
    assertFalse(settings.getBoolean(key, false));
    map.put(key, Boolean.TRUE);
    assertTrue(settings.getBoolean(key, false));
    map.put(key, "true");
    assertTrue(settings.getBoolean(key, false));
    map.put(key, new Object());
    try {
      settings.getBoolean(key, false);
      fail();
    } catch (ClassCastException e) {
      ; // to be expected
    }    
  }
  
  @Test
  public void getInt() throws Exception {
    String key = "myKey";
    assertEquals(1, settings.getInt(key, 1));
    assertEquals(2, settings.getInt(key, 2));
    map.put(key, 3);
    assertEquals(3, settings.getInt(key, 2));
    map.put(key, "4");
    assertEquals(4, settings.getInt(key, 2));
    map.put(key, new Object());
    try {
      settings.getInt(key, 9);
      fail();
    } catch (ClassCastException e) {
      ; // to be expected
    }    
  }
  
  @Test
  public void getLong() throws Exception {
    String key = "myKey";
    assertEquals(1, settings.getLong(key, 1));
    assertEquals(2, settings.getLong(key, 2));
    map.put(key, 3L);
    assertEquals(3, settings.getLong(key, 2));
    map.put(key, "4");
    assertEquals(4, settings.getLong(key, 2));
    map.put(key, new Object());
    try {
      settings.getLong(key, 9);
      fail();
    } catch (ClassCastException e) {
      ; // to be expected
    }    
  }
  
  @Test
  public void getDouble() throws Exception {
    String key = "myKey";
    assertEquals(1.0, settings.getDouble(key, 1.0), 1.0E-100);
    assertEquals(2.0, settings.getDouble(key, 2.0), 1.0E-100);
    map.put(key, 3.0);
    assertEquals(3.0, settings.getDouble(key, 2.0), 1.0E-100);
    map.put(key, "4");
    assertEquals(4.0, settings.getDouble(key, 2.0), 1.0E-100);
    map.put(key, new Object());
    try {
      settings.getDouble(key, 9.0);
      fail();
    } catch (ClassCastException e) {
      ; // to be expected
    }    
  }
  
  @Test
  public void getFloat() throws Exception {
    String key = "myKey";
    assertEquals(1.0f, settings.getFloat(key, 1.0f), 1.0E-100);
    assertEquals(2.0f, settings.getFloat(key, 2.0f), 1.0E-100);
    map.put(key, 3.0f);
    assertEquals(3.0f, settings.getFloat(key, 2.0f), 1.0E-100);
    map.put(key, "4");
    assertEquals(4.0f, settings.getFloat(key, 2.0f), 1.0E-100);
    map.put(key, new Object());
    try {
      settings.getFloat(key, 9.0f);
      fail();
    } catch (ClassCastException e) {
      ; // to be expected
    }    
  }
  
}
