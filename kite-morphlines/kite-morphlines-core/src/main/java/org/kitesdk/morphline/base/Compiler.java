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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.stdlib.DropRecordBuilder;
import org.kitesdk.morphline.stdlib.PipeBuilder;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Tool to parse and compile a morphline file or morphline config.
 */
public final class Compiler {

  private static final Object LOCK = new Object();
  
  public Compiler() {}
  
  /**
   * Parses the given morphlineFile, then finds the morphline with the given morphlineId within,
   * then compiles the morphline and returns the corresponding morphline command. The returned
   * command will feed records into finalChild.
   */
  public Command compile(File morphlineFile, String morphlineId, MorphlineContext morphlineContext, Command finalChild, Config... overrides) {
    Config config;
    try {
      config = parse(morphlineFile, overrides);
    } catch (IOException e) {
      throw new MorphlineCompilationException("Cannot parse morphline file: " + morphlineFile, null, e);
    }
    Config morphlineConfig = find(morphlineId, config, morphlineFile.getPath());
    Command morphlineCommand = compile(morphlineConfig, morphlineContext, finalChild);
    return morphlineCommand;
  }
  
  /** Loads the given config file from the local file system */
  public Config parse(File file, Config... overrides) throws IOException {
    if (file == null || file.getPath().trim().length() == 0) {
      throw new MorphlineCompilationException("Missing morphlineFile parameter", null);
    }
    if (!file.exists()) {
      throw new FileNotFoundException("File not found: " + file);
    }
    if (!file.canRead()) {
      throw new IOException("Insufficient permissions to read file: " + file);
    }
    Config config = ConfigFactory.parseFile(file);
    for (Config override : overrides) {
      config = override.withFallback(config);
    }
    
    synchronized (LOCK) {
      ConfigFactory.invalidateCaches();
      config = ConfigFactory.load(config);
      config.checkValid(ConfigFactory.defaultReference()); // eagerly validate aspects of tree config
    }
    return config;
  }

  /**
   * Finds the given morphline id within the given morphline config, using the given nameForErrorMsg
   * for error reporting.
   */
  public Config find(String morphlineId, Config config, String nameForErrorMsg) {
    List<? extends Config> morphlineConfigs = config.getConfigList("morphlines");
    if (morphlineConfigs.size() == 0) {
      throw new MorphlineCompilationException(
          "Morphline file must contain at least one morphline: " + nameForErrorMsg, null);
    }
    if (morphlineId != null) {
      morphlineId = morphlineId.trim();
    }
    if (morphlineId != null && morphlineId.length() == 0) {
      morphlineId = null;
    }
    Config morphlineConfig = null;
    if (morphlineId == null) {
      morphlineConfig = morphlineConfigs.get(0);
      Preconditions.checkNotNull(morphlineConfig);
    } else {
      for (Config candidate : morphlineConfigs) {
        if (morphlineId.equals(new Configs().getString(candidate, "id", null))) {
          morphlineConfig = candidate;
          break;
        }
      }
      if (morphlineConfig == null) {
        throw new MorphlineCompilationException(
            "Morphline id '" + morphlineId + "' not found in morphline file: " + nameForErrorMsg, null);
      }
    }
    return morphlineConfig; 
  }
  
  /**
   * Compiles the given morphline config using the given morphline context. The returned command
   * will feed records into finalChild or into /dev/null if finalChild is null.
   */
  public Command compile(Config morphlineConfig, MorphlineContext morphlineContext, Command finalChild) {
    if (finalChild == null) {
      finalChild = new DropRecordBuilder().build(null, null, null, morphlineContext);
    }
    return new PipeBuilder().build(morphlineConfig, null, finalChild, morphlineContext);
  }

}
