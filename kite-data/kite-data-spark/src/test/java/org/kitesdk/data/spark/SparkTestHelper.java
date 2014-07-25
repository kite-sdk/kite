/*
 * Copyright 2014 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Helper class to ensure that we only ever create one JavaSparkContext for use
 * in all tests.
 */
public class SparkTestHelper {

  private static JavaSparkContext sc = null;
  private static SparkConf sparkConf = null;

  public synchronized static JavaSparkContext getSparkContext() {
    if (sc == null) {
      sc = new JavaSparkContext(getSparkConf());
    }
    return sc;
  }

  public synchronized static SparkConf getSparkConf() {
    if (sparkConf == null) {
      sparkConf = new SparkConf()
          .setAppName("test")
          .setMaster("local");
    }
    return sparkConf;
  }
}
