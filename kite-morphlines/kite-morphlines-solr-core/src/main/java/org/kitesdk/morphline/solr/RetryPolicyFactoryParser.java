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
package org.kitesdk.morphline.solr;

import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.retry.DefaultRetryPolicyFactory;
import org.apache.solr.client.solrj.retry.FlexibleBoundedExponentialBackoffRetry;
import org.apache.solr.client.solrj.retry.RetryPolicyFactory;
import org.kitesdk.morphline.base.Configs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

final class RetryPolicyFactoryParser {
 
  public static final String BOUNDED_EXPONENTIAL_BACKOFF_RETRY_NAME = "BoundedExponentialBackoffRetry";
  
  private static final Logger LOG = LoggerFactory.getLogger(RetryPolicyFactoryParser.class);
      
  public RetryPolicyFactory parse(Config retryPolicyConfig) {    
    final Config boundedExponentialBackoffRetryConfig = new Configs().getConfig(retryPolicyConfig, 
        BOUNDED_EXPONENTIAL_BACKOFF_RETRY_NAME);
    final long baseSleepTime = new Configs()
        .getNanoseconds(boundedExponentialBackoffRetryConfig, "baseSleepTime", TimeUnit.MILLISECONDS.toNanos(125));
    final long maxSleepTime = new Configs()
        .getNanoseconds(boundedExponentialBackoffRetryConfig, "maxSleepTime", TimeUnit.SECONDS.toNanos(10));
    final int maxRetries = new Configs()
        .getInt(boundedExponentialBackoffRetryConfig, "maxRetries", 100);
    final long maxElapsedTime = new Configs()
        .getNanoseconds(boundedExponentialBackoffRetryConfig, "maxElapsedTime", TimeUnit.SECONDS.toNanos(3 * 60));
    LOG.debug("Parsed retry policy BoundedExponentialBackoffRetry with "
        + "baseSleepTime:{}, maxSleepTime:{}, maxRetries:{}, maxElapsedTime:{}",
        new Object[]{baseSleepTime, maxSleepTime, maxRetries, maxElapsedTime});
    if (maxRetries <= 0 || maxElapsedTime <= 0) {
      return null;
    }
    return new DefaultRetryPolicyFactory(
        new FlexibleBoundedExponentialBackoffRetry(baseSleepTime, maxSleepTime, maxRetries, maxElapsedTime));
  }

}
