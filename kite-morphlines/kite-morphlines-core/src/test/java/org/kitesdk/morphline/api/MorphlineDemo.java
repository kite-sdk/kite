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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Notifications;

/** Sample program that illustrates how to use the API to embed and execute a morphline in a host system */
public class MorphlineDemo {
  
  /** Usage: java ... <morphline.conf> <dataFile1> ... <dataFileN> */
  public static void main(String[] args) throws IOException {
    // compile morphline.conf file on the fly
    File morphlineFile = new File(args[0]);
    String morphlineId = null;
    MorphlineContext morphlineContext = new MorphlineContext.Builder().build();
    Command morphline = new Compiler().compile(morphlineFile, morphlineId, morphlineContext, null);
    
    // process each input data file
    Notifications.notifyBeginTransaction(morphline);
    try {
      for (int i = 1; i < args.length; i++) {
        InputStream in = new BufferedInputStream(new FileInputStream(new File(args[i])));
        Record record = new Record();
        record.put(Fields.ATTACHMENT_BODY, in);      
        Notifications.notifyStartSession(morphline);
        boolean success = morphline.process(record);
        if (!success) {
          System.out.println("Morphline failed to process record: " + record);
        }
        in.close();
      }
      Notifications.notifyCommitTransaction(morphline);
    } catch (RuntimeException e) {
      Notifications.notifyRollbackTransaction(morphline);
      morphlineContext.getExceptionHandler().handleException(e, null);
    }
    Notifications.notifyShutdown(morphline);
  }
}
