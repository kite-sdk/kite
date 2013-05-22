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
package com.cloudera.cdk.morphline.base;

import java.nio.charset.Charset;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.cloudera.cdk.morphline.api.MorphlineCompilationException;
import com.typesafe.config.Config;

/**
 * Helpers to traverse and read parts of a HOCON data structure.
 */
public final class Configs {
  
  private final Set<String> recognizedArguments = new LinkedHashSet();

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

}
