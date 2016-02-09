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

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;

/**
 * Pluggable API to enable users to specify exactly what kind of exceptions for what kind of solrj
 * requests shall be retried how and for how long.
 */
public interface RetryPolicyFactory {
  
  /** A retry policy that does not retry requests; Same as <code>null</code>. */
  public static final RetryPolicy DONT_RETRY = null;

  /**
   * Returns a retry policy object if the given solrj request is retryable for the given exception
   * for the given Solr server. Otherwise returns <code>null</code>, indicating that this request
   * shall not be retried, and that instead the exception shall be rethrown.
   * 
   * @param exception
   *          The exception thrown on attempting to perform the request
   * @param request
   *          The solrj request that was attempted
   * @param solrServer
   *          The solrj client proxy object that attempted to perform the request; Talks over the
   *          network to a Solr server.
   * @param currentPolicy
   *          The retry policy that has been in effect on the previous attempt to perform the
   *          request. This parameter is <code>null</code> on the first request attempt, and
   *          subsequently it is the value returned by the previous call to this method for this
   *          particular request. This parameter is guaranteed to be non-null on subsequent attempts
   *          for this particular request.
   * @return The new retry policy to use when retrying this request, or <code>null</code> if this
   *         request shall not be retried.
   */
  public RetryPolicy getRetryPolicy(
      Throwable exception, 
      SolrRequest request, 
      SolrServer solrServer,
      RetryPolicy currentPolicy);

}
