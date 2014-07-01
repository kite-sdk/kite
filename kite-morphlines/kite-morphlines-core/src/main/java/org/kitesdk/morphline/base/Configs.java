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
package org.kitesdk.morphline.base;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.kitesdk.morphline.api.MorphlineCompilationException;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

/**
 * Helpers to traverse and read parts of a HOCON data structure.
 */
public final class Configs {
  
  private final Set<String> recognizedArguments = new LinkedHashSet<String>();

  private Set<String> getRecognizedArguments() {
    return recognizedArguments;
  }
  
  private void addRecognizedArgument(String arg) {
    recognizedArguments.add(arg);
  }
  
  public void validateArguments(Config config) {
    Set<String> recognizedArgs = getRecognizedArguments();
    for (String key : config.root().keySet()) {
      if (!recognizedArgs.contains(key)) {
        throw new MorphlineCompilationException("Unrecognized command argument: " + key + 
            ", recognized arguments: " + recognizedArgs, config);
      }
    }      
  }
  
  public String getString(Config config, String path, String defaults) {
    addRecognizedArgument(path);
    if (config.hasPath(path)) {
      return config.getString(path);
    } else {
      return defaults;
    }
  }
  
  public String getString(Config config, String path) {
    addRecognizedArgument(path);
    return config.getString(path);
  }
  
  public List<String> getStringList(Config config, String path, List<String> defaults) {
    addRecognizedArgument(path);
    if (config.hasPath(path)) {
      return config.getStringList(path);
    } else {
      return defaults;
    }
  }
  
  public List<String> getStringList(Config config, String path) {
    addRecognizedArgument(path);
    return config.getStringList(path);
  }
  
  public List<? extends Config> getConfigList(Config config, String path, List<? extends Config> defaults) {
    addRecognizedArgument(path);
    if (config.hasPath(path)) {
      return config.getConfigList(path);
    } else {
      return defaults;
    }
  }

  public List<? extends Config> getConfigList(Config config, String path) {
    addRecognizedArgument(path);
    return config.getConfigList(path);
  }

  public Config getConfig(Config config, String path, Config defaults) {
    addRecognizedArgument(path);
    if (config.hasPath(path)) {
      return config.getConfig(path);
    } else {
      return defaults;
    }
  }

  public Config getConfig(Config config, String path) {
    addRecognizedArgument(path);
    return config.getConfig(path);
  }

  public boolean getBoolean(Config config, String path, boolean defaults) {
    addRecognizedArgument(path);
    if (config.hasPath(path)) {
      return config.getBoolean(path);
    } else {
      return defaults;
    }
  }
  
  public boolean getBoolean(Config config, String path) {
    addRecognizedArgument(path);
    return config.getBoolean(path);
  }
  
  public int getInt(Config config, String path, int defaults) {
    addRecognizedArgument(path);
    if (config.hasPath(path)) {
      return config.getInt(path);
    } else {
      return defaults;
    }
  }
  
  public int getInt(Config config, String path) {
    addRecognizedArgument(path);
    return config.getInt(path);
  }  

  public long getLong(Config config, String path, long defaults) {
    addRecognizedArgument(path);
    if (config.hasPath(path)) {
      return config.getLong(path);
    } else {
      return defaults;
    }
  }
  
  public long getLong(Config config, String path) {
    addRecognizedArgument(path);
    return config.getLong(path);
  }  

  public double getDouble(Config config, String path, double defaults) {
    addRecognizedArgument(path);
    if (config.hasPath(path)) {
      return config.getDouble(path);
    } else {
      return defaults;
    }
  }
  
  public double getDouble(Config config, String path) {
    addRecognizedArgument(path);
    return config.getDouble(path);
  }  

  public Charset getCharset(Config config, String path, Charset defaults) {
    String charsetName = getString(config, path, defaults == null ? null : defaults.name());
    Charset charset = charsetName == null ? null : Charset.forName(charsetName);
    return charset;
  }  

  public long getNanoseconds(Config config, String path, long defaults) {    
    addRecognizedArgument(path);
    if (config.hasPath(path)) {
      return getNanoseconds(config, path);
    } else {
      return defaults;
    }
  }
  
  public long getNanoseconds(Config config, String path) {
    addRecognizedArgument(path);
    return config.getNanoseconds(path);
  }  

  public TimeUnit getTimeUnit(Config config, String path, TimeUnit defaults) {    
    addRecognizedArgument(path);
    if (config.hasPath(path)) {
      return getTimeUnit(config, path);
    } else {
      return defaults;
    }
  }
  
  public TimeUnit getTimeUnit(Config config, String path) {
    addRecognizedArgument(path);
    return getTimeUnit(config.getString(path));
  }

  public TimeUnit getTimeUnit(String str) {
    if (!str.endsWith("s")) {
      if (str.length() > 2) {
        str = str + "s"; // normalize to plural
      }
    }
    if (str.equals("d") || str.equals("days")) {
      return TimeUnit.DAYS;
    } else if (str.equals("h") || str.equals("hours")) {
      return TimeUnit.HOURS;
    } else if (str.equals("m") || str.equals("minutes")) {
      return TimeUnit.MINUTES;
    } else if (str.equals("s") || str.equals("seconds")) {
      return TimeUnit.SECONDS;
    } else if (str.equals("ms") || str.equals("milliseconds")) {
      return TimeUnit.MILLISECONDS;
    } else if (str.equals("us") || str.equals("microseconds")) {
      return TimeUnit.MICROSECONDS;
    } else if (str.equals("ns") || str.equals("nanoseconds")) {
      return TimeUnit.NANOSECONDS;
    } else {
      throw new IllegalArgumentException("Illegal time unit: " + str);
    }
  }

  public Locale getLocale(Config config, String path, Locale defaults) {    
    addRecognizedArgument(path);
    if (config.hasPath(path)) {
      return getLocale(config, path);
    } else {
      return defaults;
    }
  }
  
  public Locale getLocale(Config config, String path) {
    addRecognizedArgument(path);
    String str = config.getString(path);
    String[] parts = Iterables.toArray(Splitter.on('_').split(str), String.class);
    if (parts.length == 1) {
      return new Locale(parts[0]);
    } else if (parts.length == 2) {
      return new Locale(parts[0], parts[1]);
    } else if (parts.length == 3) {
      return new Locale(parts[0], parts[1], parts[2]);
    } else {
      throw new MorphlineCompilationException("Illegal locale: " + str, config);
    }
  }  

  public Set<Map.Entry<String, Object>> getEntrySet(Config config) {
    Map<String, Object> map = Maps.newHashMap();
    for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
      map.put(trimQuote(entry.getKey()), entry.getValue().unwrapped());
    }
    return map.entrySet();
  }
  
  private String trimQuote(String str) {
    if (str.length() > 1 && str.startsWith("\"") && str.endsWith("\"")) {
      return str.substring(1, str.length() - 1);
    } else {
      return str;
    }
  }

}
