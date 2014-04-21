// copied and adapted from apache-solr-4.7.0
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.crunch.morphline;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.PropertyConfigurator;

import com.google.common.base.Charsets;
import com.google.common.io.Files;


final class Utils {
  
  private static final String LOG_CONFIG_FILE_CONTENTS = "hadoop.log4j.configuration.contents";
  
  public static void setLogConfigFile(File file, Configuration conf) throws IOException {
    conf.set(LOG_CONFIG_FILE_CONTENTS, Files.toString(file, Charsets.UTF_8));
  }

  public static void getLogConfigFile(Configuration conf) {
    String log4jPropertiesFileContents = conf.get(LOG_CONFIG_FILE_CONTENTS);
    if (log4jPropertiesFileContents != null) {
      Properties props = new Properties();
      try {
        props.load(new StringReader(log4jPropertiesFileContents));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      PropertyConfigurator.configure(props);
    }
  }

  public static String getShortClassName(Class clazz) {
    return getShortClassName(clazz.getName());
  }

  public static String getShortClassName(String className) {
    int i = className.lastIndexOf('.'); // regular class
    int j = className.lastIndexOf('$'); // inner class
    return className.substring(1 + Math.max(i, j));
  }
  
}
