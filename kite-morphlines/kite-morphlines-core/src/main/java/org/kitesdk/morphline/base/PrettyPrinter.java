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

import com.google.common.annotations.Beta;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;

/**
 * Utility to nicely indent and format a morphline config file.
 */
@Beta
public final class PrettyPrinter {
  
  public static void main(String[] args) {
    if (args.length ==0) {
      System.err.println("Usage: java -cp ... " + PrettyPrinter.class.getName() + " <morphlineConfigFile>");
      return;
    }
    
    Config config = ConfigFactory.parseFile(new File(args[0]));
    String str = config.root().render(
        ConfigRenderOptions.concise().setFormatted(true).setJson(false).setComments(true));

    System.out.println(str);
  }
  
}
