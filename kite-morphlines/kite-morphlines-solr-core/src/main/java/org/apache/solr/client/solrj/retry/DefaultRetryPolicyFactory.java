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

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

/**
 * A RetryPolicyFactory that's suitable for many Solr apps with idempotent request semantics.
 */
public final class DefaultRetryPolicyFactory implements RetryPolicyFactory {

  private final RetryPolicy initialRetryPolicy;
  
  public DefaultRetryPolicyFactory() {
    this(
      new FlexibleBoundedExponentialBackoffRetry(
        TimeUnit.MILLISECONDS.toNanos(125), 
        TimeUnit.SECONDS.toNanos(10),
        100, 
        TimeUnit.MINUTES.toNanos(3)));
  }
  
  /**
   * Caution: initialRetryPolicy must be thread-safe if multiple threads are concurrently calling
   * the same RetryingSolrServer.
   */
  public DefaultRetryPolicyFactory(RetryPolicy initialRetryPolicy) {
    this.initialRetryPolicy = initialRetryPolicy;
  }
  
  @Override
  public RetryPolicy getRetryPolicy(Throwable exception, SolrRequest request, SolrServer server,
      RetryPolicy currentPolicy) {
    if (exception instanceof SolrException) {
      SolrException sex = (SolrException) exception;
      if (sex.code() == ErrorCode.UNAUTHORIZED.code) {
        return RetryPolicyFactory.DONT_RETRY; // no point retrying that - would never succeed
      }
      if (sex.code() == ErrorCode.UNSUPPORTED_MEDIA_TYPE.code) {
        return RetryPolicyFactory.DONT_RETRY; // no point retrying that - would never succeed
      }
    }
    if (currentPolicy == null) {
      return initialRetryPolicy; // init
    } else {
      return currentPolicy; // continue with current policy
    }
  }

}
