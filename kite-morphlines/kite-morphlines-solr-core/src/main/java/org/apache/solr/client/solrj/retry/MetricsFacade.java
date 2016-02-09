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
package org.apache.solr.client.solrj.retry;

import java.util.concurrent.TimeUnit;

/**
 * Simple facade in front of a real metrics aggregation implementation such as codahale metrics or
 * similar.
 */
public interface MetricsFacade {

  /**
   * Increments the meter by <code>increment</code>.
   *
   * @param name
   *          the name of the meter to update
   * @param increment
   *          the amount by which the meter shall be incremented
   */
  void markMeter(String name, long increment);

  /**
   * Adds the given numeric value to a histogram.
   *
   * @param name
   *          the name of the histogram to update
   * @param value
   *          the numeric value to add to the histogram
   */
  void updateHistogram(String name, long value);

  /**
   * Adds the given duration to a timer.
   *
   * @param name
   *          the name of the timer to update
   * @param duration
   *          the length of the duration to add to the timer
   * @param unit
   *          the time unit of the <code>duration</code>
   */
  void updateTimer(String name, long duration, TimeUnit unit);

}
