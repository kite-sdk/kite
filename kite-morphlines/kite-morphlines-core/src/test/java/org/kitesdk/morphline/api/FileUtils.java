// copied from apache-commons-io-2.4
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.morphline.api;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

class FileUtils {

  public static void deleteDirectory(File directory) throws IOException {
    if (!directory.exists()) {
        return;
    }

    cleanDirectory(directory);

    if (!directory.delete()) {
        String message =
            "Unable to delete directory " + directory + ".";
        throw new IOException(message);
    }
  }
  
  public static void cleanDirectory(File directory) throws IOException {
    if (!directory.exists()) {
        String message = directory + " does not exist";
        throw new IllegalArgumentException(message);
    }

    if (!directory.isDirectory()) {
        String message = directory + " is not a directory";
        throw new IllegalArgumentException(message);
    }

    File[] files = directory.listFiles();
    if (files == null) {  // null if security restricted
        throw new IOException("Failed to list contents of " + directory);
    }

    IOException exception = null;
    for (File file : files) {
        try {
            forceDelete(file);
        } catch (IOException ioe) {
            exception = ioe;
        }
    }

    if (null != exception) {
        throw exception;
    }
  }

  public static void forceDelete(File file) throws IOException {
    if (file.isDirectory()) {
        deleteDirectory(file);
    } else {
        boolean filePresent = file.exists();
        if (!file.delete()) {
            if (!filePresent){
                throw new FileNotFoundException("File does not exist: " + file);
            }
            String message =
                "Unable to delete file: " + file;
            throw new IOException(message);
        }
    }
  }

}
