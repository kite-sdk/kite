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

import java.math.RoundingMode;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.math.LongMath;

/**
 * A retry policy that retries with exponentially increasing sleep time between retries until it has
 * reached either a) the given max number of retries or b) more than the given amount of 
 * time has elapsed since the start of the operation, whichever occurs first.
 * 
 * For example, with baseSleepTime=100, maxSleepTime=1000 we sleep as follows in each iteration:
 * random(100..199), random(200..399), random(400..799), min(random(800..1599), 1000), 1000, 1000,
 * ... and so on.
 * 
 * This implementation is thread-safe.
 */
public final class FlexibleBoundedExponentialBackoffRetry implements RetryPolicy {

  private final int maxRetries;
  private final long baseSleepTimeNanos;
  private final long maxSleepTimeNanos;
  private final long maxElapsedTimeNanos;
  private final long retriesLimit;

  /**
   * @param baseSleepTimeNanos
   *          initial amount of time to wait between retries
   * @param maxSleepTimeNanos
   *          max time in nanos to sleep on each retry
   * @param maxRetries
   *          max number of times to retry
   * @param maxElapsedTimeNanos
   *          max time in nanos to spend across all retries
   */
  public FlexibleBoundedExponentialBackoffRetry(
      long baseSleepTimeNanos, 
      long maxSleepTimeNanos, 
      int maxRetries,
      long maxElapsedTimeNanos) {
    
    Preconditions.checkArgument(baseSleepTimeNanos >= 0, 
        "baseSleepTimeNanos must not be negative: %s", baseSleepTimeNanos);
    Preconditions.checkArgument(maxSleepTimeNanos >= baseSleepTimeNanos,
        "maxSleepNanos: %s must not be less than baseSleepTimeNanos: %s", 
        maxSleepTimeNanos, baseSleepTimeNanos);
    Preconditions.checkArgument(maxRetries >= 0, 
        "maxRetries must not be negative: %s", maxRetries);
    Preconditions.checkArgument(maxElapsedTimeNanos >= 0, 
        "maxElapsedTimeNanos must not be negative: %s", maxElapsedTimeNanos);
    
    baseSleepTimeNanos = Math.max(1, baseSleepTimeNanos);
    this.baseSleepTimeNanos = baseSleepTimeNanos;
    this.maxSleepTimeNanos = maxSleepTimeNanos;
    this.maxRetries = maxRetries;
    this.maxElapsedTimeNanos = maxElapsedTimeNanos;
    this.retriesLimit = LongMath.log2(
        Long.MAX_VALUE / baseSleepTimeNanos, RoundingMode.DOWN) - 2;
  }

  @Override
  public boolean allowRetry(int retryCount, long elapsedTimeNanos, RetrySleeper sleeper) {
    if (retryCount >= 0 && 
        retryCount < maxRetries && 
        elapsedTimeNanos < maxElapsedTimeNanos) {
      try {
        sleeper.sleepFor(getSleepTime(retryCount), TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
      return true;
    }
    return false;
  }
  
  private long getSleepTime(int retryCount) {
    if (retryCount < retriesLimit) {
      long low = baseSleepTimeNanos * (1L << retryCount);
      long high = 2 * low;
      long sleepTimeNanos = ThreadLocalRandom.current().nextLong(low, high);
      return Math.min(maxSleepTimeNanos, sleepTimeNanos);
    } else {
      return maxSleepTimeNanos;
    }
  }
  
  @Override
  public String toString() {
    return String.format(
        "baseSleepTime[secs]:%1.1f, maxSleepTime[secs]:%1.1f, maxRetries:%d, "
        + "maxElapsedTime[secs]:%1.1f",
        toSeconds(baseSleepTimeNanos), 
        toSeconds(maxSleepTimeNanos), 
        maxRetries, 
        toSeconds(maxElapsedTimeNanos));
  }
  
  private double toSeconds(long nanos) {
    return nanos / (1000.0 * 1000 * 1000);
  }

}
