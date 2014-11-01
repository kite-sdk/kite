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


/**
 * Common metric names.
 */
public final class Metrics {

  public static final String NUM_PROCESS_CALLS = "numProcessCalls";
  public static final String NUM_NOTIFY_CALLS = "numNotifyCalls";
  public static final String NUM_RECORDS = "numRecords";
  public static final String NUM_FAILED_RECORDS = "numFailedRecords";
  public static final String NUM_EXCEPTION_RECORDS = "numExceptionRecords";
  public static final String NUM_CACHE_HITS = "numCacheHits";
  public static final String NUM_CACHE_MISSES = "numCacheMisses";
  public static final String NUM_FINAL_CHILD_RECORDS = "numFinalChildRecords";
  
  public static final String ELAPSED_TIME = "elapsedTime";
  //public static final String ELAPSED_TOTAL_TIME = "elapsedTotalTime";
  
  public static final String MORPHLINE_APP = "morphline.app";
}
