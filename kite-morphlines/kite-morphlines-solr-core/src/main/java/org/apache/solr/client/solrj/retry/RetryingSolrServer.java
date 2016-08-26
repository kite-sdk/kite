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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;


/**
 * A wrapper around an underlying solrj {@link SolrClient} object; On exceptions, the wrapper
 * retries solrj requests according to the given configurable {@link RetryPolicyFactory}.
 * 
 * This is useful in case solr servers are temporarily overloaded, or if there are transient network
 * issues, etc.
 * 
 * For example, this class can be configured with a {@link DefaultRetryPolicyFactory} in combination
 * with the {@link FlexibleBoundedExponentialBackoffRetry}. Metrics events are forwarded to a
 * {@link MetricsFacade} object.
 */
public class RetryingSolrServer extends SolrClient {

  private final SolrClient solrServer;
  
  private final RetryPolicyFactory retryPolicyFactory;

  private final MetricsFacade metrics;

  private final CountDownLatch isShuttingDown = new CountDownLatch(1);
  
  private final RetrySleeper sleeper = new RetrySleeper() {
    @Override
    public void sleepFor(long time, TimeUnit unit) throws InterruptedException {
      isShuttingDown.await(time, unit);
    }
  };
  
  private final AtomicLong totalRetryCount = new AtomicLong(0);
  
  private final AtomicLong totalRequestCount = new AtomicLong(0);
  
  private static final String ROOT_PREFIX = RetryingSolrServer.class.getSimpleName() + ".";

  private static final int MAX_STRING_LENGTH = Integer.parseInt(System.getProperty(
      RetryingSolrServer.class.getSimpleName() +".maxStringLength", "300"));
  
  private static final boolean REDACT_QUERY_REQUEST_PARAMS = "true".equals(System.getProperty(
      RetryingSolrServer.class.getSimpleName() +".redactQueryRequestParams", "true"));

  private static final boolean ENABLE_LOGGING = "true".equals(System.getProperty(
      RetryingSolrServer.class.getSimpleName() +".enableLogging", "true"));

  private static final Pattern WHITESPACE_EXCEPT_SPACE_PATTERN = Pattern.compile(
      "[\\t\\n\\x0B\\f\\r]");
  
  private static final Pattern TWO_OR_MORE_SPACES_PATTERN = Pattern.compile(
      "  +"); // two or more spaces

  private static final Pattern EXCEPTION_HOST_PATTERN = Pattern.compile(
      "(UnknownHostException: )([0-9A-Za-z][0-9A-Za-z-]{0,62})((?:\\.[0-9A-Za-z][0-9A-Za-z-]{0,62}){2,}?)");

  // Note: %3A%2F%2F is an encoding of :// used in some Solr exception messages
  private static final Pattern URL_SCHEME_AND_AUTHORITY_PATTERN = Pattern.compile(
      "([A-Za-z][0-9A-Za-z+-.]{0,62}(?:://|%3A%2F%2F))([0-9A-Za-z][0-9A-Za-z-]{0,62})((?:\\.[0-9A-Za-z][0-9A-Za-z-]{0,62})*)");
  
  private static final Pattern SHARD_AND_REPLICA_PATTERN = Pattern.compile(
      "(_(?:shard|SHARD))[0-9]+(_(?:replica|REPLICA))[0-9]+");

  private static final Pattern COLLECTION_AND_SLICE_PATTERN = Pattern.compile(
      "(collection: \\S+ slice: shard)\\d+");

  private static final Pattern NOW_TIMESTAMP_PATTERN = Pattern.compile(
      "(NOW=)\\d+"); // part of Solr query params

  private static final Logger LOG = LoggerFactory.getLogger(RetryingSolrServer.class);
    
  /**
   * Constructs an instance with the given parameters.
   *
   * Caution: retryPolicyFactory and metrics params must be thread-safe if multiple threads are
   * concurrently calling the same {@link RetryingSolrServer}.
   */
  public RetryingSolrServer(SolrClient solrServer, 
                            RetryPolicyFactory retryPolicyFactory, 
                            MetricsFacade metrics) {
    if (solrServer == null) {
      throw new IllegalArgumentException("solrServer param must not be null");
    }
    this.solrServer = solrServer;
    if (retryPolicyFactory == null) {
      throw new IllegalArgumentException("retryPolicyFactory param must not be null");
    }
    this.retryPolicyFactory = retryPolicyFactory;
    if (metrics == null) {
      metrics = new NullMetricsFacade(); 
    }
    this.metrics = metrics;
  }

  /** Returns the backing solr proxy */
  public final SolrClient getUnderlyingSolrServer() {
    return solrServer;
  }
  
  protected final MetricsFacade getMetrics() {
    return metrics;
  }
  
  /**
   * Returns the number of nano seconds to wait until starting execution of this request. This
   * default implementation returns zero to indicate that the request shall be started immediately.
   * 
   * Override this method for custom behaviour, e.g. using Guava's RateLimiter.reserve(1).
   */
  protected long limitRequestRate(SolrRequest request) {
    return 0;
  }

  @Override
  public NamedList<Object> request(final SolrRequest request, String collection) throws SolrServerException, IOException {
    int retryCount = 0;
    RetryPolicy retryPolicy = null;
    totalRequestCount.incrementAndGet();
    getMetrics().markMeter(ROOT_PREFIX + "requests", 1);
    Map<String, MutableLong> exceptionRootCauseCounters = new HashMap<String, MutableLong>();
    final long startTime = System.nanoTime();
    while (true) {
      final long nanosToWaitForRateLimiter = limitRequestRate(request);
      try {
        isShuttingDown.await(nanosToWaitForRateLimiter, TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
        ; // ignore
      }

      final long lastStartTime = System.nanoTime();
      NamedList response;
      try {
        response = solrServer.request(request, collection);
      } catch (Exception exception) {
        long requestDuration = System.nanoTime() - lastStartTime;
        String exceptionTopLevelMsg = limitStringLength(getExceptionKey(exception));
        String exceptionRootCauseMsg = limitStringLength(getExceptionKey(getRootCause(exception)));
        String requestKey = limitStringLength(getRequestKey(request));
        handleException(request, exceptionTopLevelMsg, exceptionRootCauseMsg, requestKey, requestDuration);
        String reason = "n/a";
        boolean retry = !isShuttingDown();
        if (!retry) {
          reason = "Solrj client is shutting down";
        } else {
          retryPolicy = retryPolicyFactory.getRetryPolicy(exception, request, solrServer, retryPolicy);
          retry = retryPolicy != null;
          if (!retry) {
            reason = "Exception is non-retryable per RetryPolicyFactory {" + retryPolicyFactory + "}";
          } else {
            if (ENABLE_LOGGING) {
              LOG.warn("Retryable exception received: ", exception);
            }            
            retry = retryPolicy.allowRetry(
                retryCount, 
                System.nanoTime() - startTime, 
                sleeper);
            if (!retry) {
              reason = "Request must not be retried " + (retryCount > 0 ? "anymore " : "") 
                     + "per RetryPolicy {" + retryPolicy + "}";
            } else {
              retry = !isShuttingDown(); // recheck
              if (!retry) {
                reason = "Solrj client is shutting down(2)";
              }
            }
          }
        }
        
        if (!retry) {
          // rethrow exception with added details to help users diagnose the situation
          long secs = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime);
          throw new RetriesExhaustedException(
              "Performed " + totalRetryCount + " retries across " + totalRequestCount
              + " client requests. Gave up because the last " + retryCount + " retries across " + secs
              + " seconds for the current request failed with this reason: '" + reason + "' and these"
              + " root causes: ",
              exceptionRootCauseCounters, 
              exception);
        } 
        
        if (ENABLE_LOGGING) {
          LOG.info("Retrying request: {}", requestKey);
        }
        assert exception != null;
        retryCount++;
        totalRetryCount.incrementAndGet();
        incCounter(exceptionRootCauseCounters, exceptionRootCauseMsg);
        metrics.markMeter(ROOT_PREFIX + "retries", 1);
        continue; // retry request by continuing while(true) retry loop
      }
      
      // solr request succeeded without throwing an exception 
      long requestDuration = System.nanoTime() - lastStartTime;
      handleSuccess(request, response, retryCount, startTime, requestDuration);
      return response;
    }
  }

  private void handleSuccess(SolrRequest request, NamedList response, int retryCount, long startTime,
      long requestDuration) {
    String successPrefix = ROOT_PREFIX + "success.";
    if (retryCount > 0) {
      String retriedRequestsPrefix = successPrefix + "retry.";
      long totalRequestDuration = System.nanoTime() - startTime;
      metrics.updateHistogram(retriedRequestsPrefix + "count", retryCount);
      metrics.updateTimer(retriedRequestsPrefix + "time", totalRequestDuration, TimeUnit.NANOSECONDS);
//    metrics.updateHistogram(retriedRequestsPrefix + "count.req." + requestKey, retryCount);
//    metrics.updateHistogram(retriedRequestsPrefix + "count.top." + firstExceptionTopLevelMsg, retryCount);
//    metrics.updateHistogram(retriedRequestsPrefix + "count.root." + firstExceptionRootCauseMsg, retryCount);
//    metrics.updateTimer(retriedRequestsPrefix + "time.req." + requestKey, totalRequestDuration, TimeUnit.NANOSECONDS);
//    metrics.updateTimer(retriedRequestsPrefix + "time.top." + firstExceptionTopLevelMsg, totalRequestDuration, TimeUnit.NANOSECONDS);
//    metrics.updateTimer(retriedRequestsPrefix + "time.root." + firstExceptionRootCauseMsg, totalRequestDuration, TimeUnit.NANOSECONDS);
    }
    metrics.updateHistogram(successPrefix + "anycount", retryCount);
    metrics.updateTimer(ROOT_PREFIX + "time", requestDuration, TimeUnit.NANOSECONDS);
  }

  private void handleException(SolrRequest request, String exceptionTopLevelMsg, String exceptionRootCauseMsg,
      String requestKey, long requestDuration) {
    String timePrefix = "time.";
    metrics.updateTimer(ROOT_PREFIX + "time", requestDuration, TimeUnit.NANOSECONDS);
    String failedRequestsPrefix = ROOT_PREFIX + "fail.";
    metrics.updateTimer(failedRequestsPrefix + "time", requestDuration, TimeUnit.NANOSECONDS);
    metrics.updateTimer(failedRequestsPrefix + timePrefix + "req." + requestKey, requestDuration, TimeUnit.NANOSECONDS);
    metrics.updateTimer(failedRequestsPrefix + timePrefix + "xreq.top." + requestKey + "." + exceptionTopLevelMsg, requestDuration, TimeUnit.NANOSECONDS);
    metrics.updateTimer(failedRequestsPrefix + timePrefix + "xreq.root." + requestKey + "." + exceptionRootCauseMsg, requestDuration, TimeUnit.NANOSECONDS);
    metrics.updateTimer(failedRequestsPrefix + timePrefix + "top." + exceptionTopLevelMsg, requestDuration, TimeUnit.NANOSECONDS);
    metrics.updateTimer(failedRequestsPrefix + timePrefix + "root." + exceptionRootCauseMsg, requestDuration, TimeUnit.NANOSECONDS);
  }

  protected String getRequestKey(SolrRequest request) {
    String str = request.getClass().getSimpleName() + ".";
    return REDACT_QUERY_REQUEST_PARAMS  && request instanceof QueryRequest ? 
      str + "<redacted>" : 
      str + normalizeRequestKey(String.valueOf(request.getParams()));
  }

  @VisibleForTesting
  final String normalizeRequestKey(String str) {
    str = NOW_TIMESTAMP_PATTERN.matcher(str).replaceAll("$1<redacted>");
    str = SHARD_AND_REPLICA_PATTERN.matcher(str).replaceAll("$1<redacted>$2<redacted>");
    str = normalizeWhitespace(str);
    return str;
  }

  /**
   * Make metric names a bit more human readable.
   * Examples:     
   * "foo  bar" -> "foo bar"
   * "foo\nbar" -> "foo bar"
   * "foo\n\tbar" -> "foo bar"
   */
  private String normalizeWhitespace(String str) {
    str = WHITESPACE_EXCEPT_SPACE_PATTERN.matcher(str).replaceAll(" ");
    str = TWO_OR_MORE_SPACES_PATTERN.matcher(str).replaceAll(" ");
    return str;
  }
  
  protected String getExceptionKey(Throwable exception) {
    return normalizeExceptionKey(exception.toString());
  }
  
  /**
   * Coalesce exception messages containing HTTP REST calls to all server shards into a single bucket.
   * This greatly reduces the amount of metric spam.
   * Example: https://vb0706.foo.bar.com:8983 --> https://<redacted>.foo.bar.com:8983
   * Example: UnknownHostException: vb0706.foo.bar.com -> UnknownHostException: <redacted>.foo.bar.com
   * Example: insertAndQueryTweets_shard30_replica91 --> insertAndQueryTweets_shard<redacted>_replica<redacted>
   * Example: collection: collection1 slice: shard130 --> collection: collection1 slice: shard<redacted>
   * Example: &NOW=1448689151133 -> &NOW=<redacted>
   */
  private String normalizeExceptionKey(String str) {
    str = URL_SCHEME_AND_AUTHORITY_PATTERN.matcher(str).replaceAll("$1<redacted>$3");
    str = SHARD_AND_REPLICA_PATTERN.matcher(str).replaceAll("$1<redacted>$2<redacted>");
    str = EXCEPTION_HOST_PATTERN.matcher(str).replaceAll("$1<redacted>$3");
    str = COLLECTION_AND_SLICE_PATTERN.matcher(str).replaceAll("$1<redacted>");
    str = NOW_TIMESTAMP_PATTERN.matcher(str).replaceAll("$1<redacted>");
    str = normalizeWhitespace(str);
    str = omitPackageName(str, "java.io.");
    str = omitPackageName(str, "java.net.");
    str = omitPackageName(str, "org.apache.solr.client.solrj.impl.");
    //str = omitPackageName(str, "org.apache.http.client.");    
    return str;
  }
  
  private String omitPackageName(String str, String prefix) {
    return str.startsWith(prefix) ? str.substring(prefix.length()) : str;
  }

  /** prevents gigantic messages */
  private String limitStringLength(String str) {
    if (str.length() > MAX_STRING_LENGTH) {
      str = str.substring(0, MAX_STRING_LENGTH) + " ..."; 
    }
    return str;
  }

  private void incCounter(Map<String, MutableLong> counters, String key) {
    MutableLong counter = counters.get(key);
    if (counter == null) {
      counter = new MutableLong(0);
      counters.put(key, counter);
    }
    counter.inc(1);
  }

  private Throwable getRootCause(Throwable throwable) {
    Throwable rootCause = ExceptionUtils.getRootCause(throwable); 
    if (rootCause != null) {
      return rootCause;
    } else {
      return throwable;
    }
  }
  
  private boolean isShuttingDown() {
    try {
      return isShuttingDown.await(0, TimeUnit.NANOSECONDS);
    } catch (InterruptedException e) {
      return true;
    }
  }
  
  @Override
  public DocumentObjectBinder getBinder() {
    return solrServer.getBinder();
  }

  @Override
  public void close() {
    isShuttingDown.countDown();
    try {
      solrServer.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  /**
   * A {@link MetricsFacade} implementation that ignores all metrics events - does nothing.
   */
  private static final class NullMetricsFacade implements MetricsFacade {

    @Override
    public void markMeter(String name, long increment) {}

    @Override
    public void updateHistogram(String name, long value) {}

    @Override
    public void updateTimer(String name, long duration, TimeUnit unit) {}        

  };
    
}
