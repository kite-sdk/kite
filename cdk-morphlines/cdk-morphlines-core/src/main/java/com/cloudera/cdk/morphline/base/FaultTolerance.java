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
package com.cloudera.cdk.morphline.base;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.cdk.morphline.api.ExceptionHandler;
import com.cloudera.cdk.morphline.api.MorphlineRuntimeException;
import com.cloudera.cdk.morphline.api.Record;
import com.google.common.annotations.Beta;

/**
 * Mission critical, large-scale online production systems need to make progress without downtime
 * despite some issues.
 * 
 * Some program exceptions tend to be transient, in which case the corresponding task can be
 * retried. Examples include network connection errors, timeouts, etc. These are called recoverable
 * exceptions.
 * 
 * The isIgnoringRecoverableExceptions should only be enabled if an exception misclassification bug
 * has been identified.
 */
@Beta
public final class FaultTolerance implements ExceptionHandler {
    
  private final boolean isProductionMode; 
  private final boolean isIgnoringRecoverableExceptions;
  private final Class[] clazzes;
  
  public static final String IS_PRODUCTION_MODE = "isProductionMode";
  public static final String IS_IGNORING_RECOVERABLE_EXCEPTIONS = "isIgnoringRecoverableExceptions";
  public static final String RECOVERABLE_EXCEPTION_CLASSES = "recoverableExceptionClasses";

  private static final Logger LOG = LoggerFactory.getLogger(FaultTolerance.class);

  public FaultTolerance(boolean isProductionMode, boolean isIgnoringRecoverableExceptions) {
    this(isProductionMode, isIgnoringRecoverableExceptions, null);
  }
  
  public FaultTolerance(boolean isProductionMode, boolean isIgnoringRecoverableExceptions, String classesStr) {
    this.isProductionMode = isProductionMode;
    this.isIgnoringRecoverableExceptions = isIgnoringRecoverableExceptions;
    if (classesStr != null) {
      classesStr = classesStr.trim();
      if (classesStr.length() == 0) {
        classesStr = null;
      }
    }
    String[] classes;
    if (classesStr == null) {
      classes = new String[0];
    } else {
      classes = classesStr.split(",");
    }
    clazzes = new Class[classes.length];
    for (int i = 0; i < classes.length; i++) {
      try {
        clazzes[i] = Class.forName(classes[i].trim());
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  @Override
  public void handleException(Throwable t, Record record) {
    if (t instanceof Error) {
      throw (Error) t; // never ignore errors
    }
    if (isProductionMode()) {
      if (!isRecoverableException(t)) {
        LOG.warn("Ignoring unrecoverable exception in production mode for record: " + record, t);
        return;
      } else if (isIgnoringRecoverableExceptions()) {
        LOG.warn("Ignoring recoverable exception in production mode for record: " + record, t);
        return;
      }
    }
    throw new MorphlineRuntimeException(t);
  }

  private boolean isProductionMode() {
    return isProductionMode;
  }
  
  private boolean isIgnoringRecoverableExceptions() {
    return isIgnoringRecoverableExceptions;
  }
  
  private boolean isRecoverableException(Throwable t) {
    while (true) {
      for (Class clazz : clazzes) {
        if (clazz.isAssignableFrom(t.getClass())) {
          return true;
        }
      }      
      Throwable cause = t.getCause();
      if (cause == null || cause == t) {
        return false;
      }
      t = cause;
    } 

  }
    
}
