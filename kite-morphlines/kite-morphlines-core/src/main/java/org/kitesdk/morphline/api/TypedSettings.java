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

/**
 * A Map with convenient typed accessors for reading values.
 */
public final class TypedSettings {
  
  public static final String DRY_RUN_SETTING_NAME = "isDryRun";
  public static final String TASK_CONTEXT_SETTING_NAME = "taskContext";

  private final Map<String, Object> map;
  
  TypedSettings(Map<String, Object> map) {
    this.map = map;
  }

  public String getString(String key, String defaultValue) {
    Object value = map.get(key);
    if (value == null) {
      return defaultValue;
    } else if (value instanceof String) {
      return (String) value;
    } else {
      throw new ClassCastException(errorMessage(key, value, "String"));
    }
  }

  public boolean getBoolean(String key, boolean defaultValue) {
    Object value = map.get(key);
    if (value == null) {
      return defaultValue;
    } else if (value instanceof Boolean) {
      return (Boolean) value;
    } else if (value instanceof String) {
      return Boolean.parseBoolean((String) value);
    } else {
      throw new ClassCastException(errorMessage(key, value, "boolean"));
    }
  }

  public int getInt(String key, int defaultValue) {
    Object value = map.get(key);
    if (value == null) {
      return defaultValue;
    } else if (value instanceof Integer) {
      return (Integer) value;
    } else if (value instanceof String) {
      return Integer.parseInt((String) value);
    } else {
      throw new ClassCastException(errorMessage(key, value, "integer"));
    }
  }

  public long getLong(String key, int defaultValue) {
    Object value = map.get(key);
    if (value == null) {
      return defaultValue;
    } else if (value instanceof Long) {
      return (Long) value;
    } else if (value instanceof String) {
      return Long.parseLong((String) value);
    } else {
      throw new ClassCastException(errorMessage(key, value, "long"));
    }
  }

  public double getDouble(String key, double defaultValue) {
    Object value = map.get(key);
    if (value == null) {
      return defaultValue;
    } else if (value instanceof Double) {
      return (Double) value;
    } else if (value instanceof String) {
      return Double.parseDouble((String) value);
    } else {
      throw new ClassCastException(errorMessage(key, value, "double"));
    }
  }

  public float getFloat(String key, float defaultValue) {
    Object value = map.get(key);
    if (value == null) {
      return defaultValue;
    } else if (value instanceof Float) {
      return (Float) value;
    } else if (value instanceof String) {
      return Float.parseFloat((String) value);
    } else {
      throw new ClassCastException(errorMessage(key, value, "float"));
    }
  }

  public Object getObject(String key, Object defaultValue) {
    Object value = map.get(key);
    if (value == null) {
      return defaultValue;
    } else {
      return value;
    }
  }

  private String errorMessage(String key, Object value, String type) {
    return "key:value " + key + ":" + value + " cannot be converted to a " + type;
  }
  
}
