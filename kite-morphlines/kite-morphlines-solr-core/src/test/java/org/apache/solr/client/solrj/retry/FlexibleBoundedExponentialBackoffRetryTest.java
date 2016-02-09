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

import org.junit.Assert;
import org.junit.Test;


public class FlexibleBoundedExponentialBackoffRetryTest extends Assert {

  @Test
  public void testBasic() throws Exception {
    long baseSleepTimeNanos = 100;
    long maxSleepNanos = 500;
    int maxRetries = 100;
    long maxElapsedTimeNanos = 5000;
    RetryPolicy policy = new FlexibleBoundedExponentialBackoffRetry(
        baseSleepTimeNanos, maxSleepNanos, maxRetries, maxElapsedTimeNanos);
    long elapsedTimeNanos = 10;
    CountingSleeper sleeper = new CountingSleeper();
    for (int retryCount = 0; retryCount < maxRetries + 5; retryCount++) {
      boolean retry = policy.allowRetry(retryCount, elapsedTimeNanos, sleeper);
      if (retryCount < maxRetries) {
        assertTrue(retry);
        assertEquals(retryCount + 1, sleeper.count);
      } else {
        assertFalse(retry);
        assertEquals(maxRetries, sleeper.count);
      }
      assertTrue(sleeper.time >= baseSleepTimeNanos);
    }
    
    policy = new FlexibleBoundedExponentialBackoffRetry(
        TimeUnit.SECONDS.toNanos(2), TimeUnit.SECONDS.toNanos(3), 100, TimeUnit.SECONDS.toNanos(4));
    assertEquals(
        "baseSleepTime[secs]:2.0, maxSleepTime[secs]:3.0, maxRetries:100, maxElapsedTime[secs]:4.0", 
        policy.toString());
    
    policy = new FlexibleBoundedExponentialBackoffRetry(0, 0, maxRetries, maxElapsedTimeNanos);
    sleeper = new CountingSleeper();
    assertTrue(policy.allowRetry(0, elapsedTimeNanos, sleeper));
    assertEquals(1, sleeper.count);
    assertEquals(0, sleeper.time);
    
    assertFalse(policy.allowRetry(-1, elapsedTimeNanos, sleeper)); // negative retryCount
    assertFalse(policy.allowRetry(0, maxElapsedTimeNanos, sleeper)); // reached maxElapsedTimeMs 
    assertTrue(policy.allowRetry(0, 0, sleeper)); 
    
    policy = new FlexibleBoundedExponentialBackoffRetry(
        Long.MAX_VALUE, Long.MAX_VALUE, maxRetries, maxElapsedTimeNanos);
    sleeper = new CountingSleeper();
    assertTrue(policy.allowRetry(0, elapsedTimeNanos, sleeper));
    assertEquals(1, sleeper.count);
    assertEquals(Long.MAX_VALUE, sleeper.time);
    
    try {
      new FlexibleBoundedExponentialBackoffRetry(
          -1, maxSleepNanos-1, maxRetries, maxElapsedTimeNanos);
      fail();
    } catch (IllegalArgumentException e) {
      ; // expected
    }
    
    try {
      new FlexibleBoundedExponentialBackoffRetry(
          baseSleepTimeNanos, maxSleepNanos, -1, maxElapsedTimeNanos);
      fail();
    } catch (IllegalArgumentException e) {
      ; // expected
    }
    
    try {
      new FlexibleBoundedExponentialBackoffRetry(
          baseSleepTimeNanos, baseSleepTimeNanos-1, maxRetries, maxElapsedTimeNanos);
      fail();
    } catch (IllegalArgumentException e) {
      ; // expected
    }
    
    try {
      new FlexibleBoundedExponentialBackoffRetry(
          baseSleepTimeNanos, maxSleepNanos, maxRetries, -1);
      fail();
    } catch (IllegalArgumentException e) {
      ; // expected
    }
    
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class CountingSleeper implements RetrySleeper {
    
    int count = 0;
    long time = 0;

    @Override
    public void sleepFor(long time, TimeUnit unit) throws InterruptedException {
      this.count++;
      this.time += time;
    }
    
  }
  
}
