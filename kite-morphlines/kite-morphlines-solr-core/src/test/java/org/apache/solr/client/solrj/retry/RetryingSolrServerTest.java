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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RetryingSolrServerTest extends SolrTestCaseJ4 {

  private SolrServer solrServer;
  
  private static final String RESOURCES_DIR = "target" + File.separator + "test-classes";
  private static final String DEFAULT_BASE_DIR = "solr";
  
  private static final Logger LOG = LoggerFactory.getLogger(RetryingSolrServerTest.class);

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore(
        "solrconfig.xml", 
        "schema.xml",
        RESOURCES_DIR + File.separator + DEFAULT_BASE_DIR
    );    
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    solrServer = new EmbeddedSolrServer(h.getCoreContainer(), "");
  }
  
  @Test
  public void testNoRetries() throws Exception {
    SolrQuery query = getDefaultQuery();
    solrServer.query(query);
    FailingSolrServer failingSolrServer = new FailingSolrServer(solrServer);
    SolrServer solr = new RetryingSolrServer(failingSolrServer, getNoRetryPolicyFactory(), getMetricsFacade());
    try {
      solr.query(query);
      fail();
    } catch (RetriesExhaustedException e) {
      assertTrue(e.getCause() instanceof FailingSolrServer.InjectedSolrServerException);
      Assert.assertEquals(1, failingSolrServer.getNumRequests());
      Assert.assertEquals(1, failingSolrServer.getNumInjectedFailures());
    }    
  }
  
  @Test
  public void testRetryTwice() throws Exception {
    SolrQuery query = getDefaultQuery();
    solrServer.query(query);
    FailingSolrServer failingSolrServer = new FailingSolrServer(solrServer);
    SolrServer solr = new RetryingSolrServer(failingSolrServer, getRetryTwicePolicyFactory(), getMetricsFacade());
    try {
      solr.query(query);
      fail();
    } catch (RetriesExhaustedException e) {
      Assert.assertTrue(e.getCause() instanceof FailingSolrServer.InjectedSolrServerException);
      Assert.assertEquals(3, failingSolrServer.getNumRequests());
      Assert.assertEquals(3, failingSolrServer.getNumInjectedFailures());
      LOG.info("RetriesExhaustedException.getMessage(): " + e.getMessage());
    }    
  }
  
  @Test
  public void testRetries() throws Exception {
    new DefaultRetryPolicyFactory();
    SolrQuery query = getDefaultQuery();
    FailingSolrServer failingSolrServer = new FailingSolrServer(solrServer);
    SolrServer solr = new RetryingSolrServer(
        failingSolrServer, 
        new DefaultRetryPolicyFactory(new FlexibleBoundedExponentialBackoffRetry(
            TimeUnit.MILLISECONDS.toNanos(1), 
            TimeUnit.MILLISECONDS.toNanos(1000), 
            20, 
            TimeUnit.MINUTES.toNanos(5))),
        getMetricsFacade()
        );
    Assert.assertNotNull(solr.query(query));
    Assert.assertEquals(FailingSolrServer.SUCCESS, failingSolrServer.getNumRequests());
    Assert.assertEquals(FailingSolrServer.SUCCESS - 1, failingSolrServer.getNumInjectedFailures());
    solr.query(query);
    Assert.assertEquals(FailingSolrServer.SUCCESS + 1, failingSolrServer.getNumRequests());
    Assert.assertEquals(FailingSolrServer.SUCCESS - 1, failingSolrServer.getNumInjectedFailures());

    failingSolrServer.reset();
    Assert.assertNotNull(solr.query(query));
    Assert.assertEquals(FailingSolrServer.SUCCESS, failingSolrServer.getNumRequests());
    Assert.assertEquals(FailingSolrServer.SUCCESS - 1, failingSolrServer.getNumInjectedFailures());
    solr.query(query);
    Assert.assertEquals(FailingSolrServer.SUCCESS + 1, failingSolrServer.getNumRequests());
    Assert.assertEquals(FailingSolrServer.SUCCESS - 1, failingSolrServer.getNumInjectedFailures());
    
    // verify that after shutdown() is called, requests fail immediately without retries 
    failingSolrServer.reset();
    solr.shutdown();
    try { 
      solr.query(query);
      fail();
    } catch (RetriesExhaustedException e) {
      assertTrue(e.getCause() instanceof FailingSolrServer.InjectedSolrServerException);
      Assert.assertEquals(1, failingSolrServer.getNumRequests());
      Assert.assertEquals(1, failingSolrServer.getNumInjectedFailures());
    }
  }

  @Test
  public void testRetryOfBadRequest() throws Exception {
    SolrInputDocument doc = new SolrInputDocument(); // bad doc is missing 'id' field

    // without RetryingSolrServer
    try {
      solrServer.add(doc); 
      fail();
    } catch (SolrException e) {
      assertEquals(ErrorCode.BAD_REQUEST.code, e.code());
    }
        
    // RetryingSolrServer, retry twice
    CountingSolrServer countingSolrServer = new CountingSolrServer(solrServer);
    SolrServer solr = new RetryingSolrServer(countingSolrServer, getRetryTwicePolicyFactory(), getMetricsFacade());
    try {
      solr.add(doc);
      fail();
    } catch (RetriesExhaustedException e) {
      Assert.assertEquals(3, countingSolrServer.getNumRequests());
      Assert.assertTrue(e.getCause() instanceof SolrException);
      SolrException sex = (SolrException) e.getCause(); 
      Assert.assertEquals(ErrorCode.BAD_REQUEST.code, sex.code());
    }    
  }
  
  @Test
  public void testGetUnderlyingSolrServer() throws Exception {
    RetryingSolrServer retryingSolr = new RetryingSolrServer(solrServer, getNoRetryPolicyFactory(), getMetricsFacade());
    assertSame(solrServer, retryingSolr.getUnderlyingSolrServer());
  }
  
  @Test
  public void testGetBinder() throws Exception {
    RetryingSolrServer retryingSolr = new RetryingSolrServer(solrServer, getNoRetryPolicyFactory(), getMetricsFacade());
    assertSame(solrServer.getBinder(), retryingSolr.getBinder());
  }

  @Test
  public void testNormalizeRequestKey() throws Exception {
    assertEquals("&NOW=<redacted>", normalizeRequestKey("&NOW=1448689151133"));    
  }

  @Test
  public void testGetExceptionKey() throws Exception {
    // matching:
    assertEquals(" UnknownHostException: <redacted>.foo.bar.com ", getExceptionKey(" UnknownHostException: vb0706.foo.bar.com "));
    assertEquals(" collection: collection1 slice: shard<redacted> ", getExceptionKey(" collection: collection1 slice: shard130 "));
    assertEquals(" _SHARD<redacted>_REPLICA<redacted> ", getExceptionKey(" _SHARD110_REPLICA3 "));

    assertEquals("Xhttps://<redacted>.foo.bar.com:8983y", getExceptionKey("Xhttps://vb0706.foo.bar.com:8983y"));
    assertEquals("Xhttp://<redacted>.foo.bar.com", getExceptionKey("Xhttp://vb0706.foo.bar.com")); // no port
    assertEquals("Xhttp://<redacted>.foo.bar.com/", getExceptionKey("Xhttp://vb0706.foo.bar.com/")); // no port
    assertEquals("Xhttp://<redacted>.foo.bar.com]", getExceptionKey("Xhttp://vb0706.foo.bar.com]")); // no port
    assertEquals("Xhttp%3A%2F%2F<redacted>.foo.bar.com]", getExceptionKey("Xhttp%3A%2F%2Fvb0706.foo.bar.com]")); // escaped ://
    
    assertEquals("ftp://<redacted>.foo.bar:8983/some/path", getExceptionKey("ftp://vb0706.foo.bar:8983/some/path")); // with path
    assertEquals("Ftp+-.://<redacted>.foo.bar:8983/some/path", getExceptionKey("Ftp+-.://vb0706.foo.bar:8983/some/path")); // scheme with plus, minus and dot
    
    assertEquals("insertAndQueryTweets_shard<redacted>_replica<redacted>", getExceptionKey("insertAndQueryTweets_shard30_replica3"));

    assertEquals("foo bar", getExceptionKey("foo  bar")); // whitespace
    assertEquals("foo bar", getExceptionKey("foo\nbar")); // whitespace
    assertEquals("foo bar", getExceptionKey("foo\n\tbar")); // whitespace

    assertEquals("IOException msg", getExceptionKey("java.io.IOException msg")); // omit package name of well known Exception classes

    // not matching:
    String str;
    assertEquals(str = "Xhttps:/vb0706.foo.bar.com:8983y", getExceptionKey(str)); // missing ://
    assertEquals(str = "://vb0706.foo.bar.com:8983y", getExceptionKey(str)); // missing scheme
    assertEquals(str = "foo", getExceptionKey(str));
  }
  
  private String getExceptionKey(final String str) {
    RetryingSolrServer solr = new RetryingSolrServer(solrServer, getNoRetryPolicyFactory(), getMetricsFacade());
    return solr.getExceptionKey(new Throwable() {
      @Override
      public String toString() {
        return str;
      }          
    });    
  }
  
  private String normalizeRequestKey(String str) {
    RetryingSolrServer solr = new RetryingSolrServer(solrServer, getNoRetryPolicyFactory(), getMetricsFacade());
    return solr.normalizeRequestKey(str);
  }
  
  private RetryPolicyFactory getRetryTwicePolicyFactory() {
    return new RetryPolicyFactory() {

      @Override
      public RetryPolicy getRetryPolicy(Throwable exception, SolrRequest request, SolrServer server,
          RetryPolicy currentPolicy) {
        if (currentPolicy == null) {
          return new FlexibleBoundedExponentialBackoffRetry(0, 0, 2, Long.MAX_VALUE);
        } else {
          return currentPolicy;
        }
      }
      
    };
  }
  
  private RetryPolicyFactory getNoRetryPolicyFactory() {
    return new RetryPolicyFactory() {

      @Override
      public RetryPolicy getRetryPolicy(Throwable exception, SolrRequest request, SolrServer server,
          RetryPolicy currentPolicy) {
        return null;
      }
      
    };
  }
  
  private SolrQuery getDefaultQuery() {
    return new SolrQuery("*:*").addFilterQuery("id:choreRunner_12_56");
  }
  
  private MetricsFacade getMetricsFacade() {
//    return new CodahaleMetricsFacade(new MetricRegistry(), new TDigestMetricBuilders());
    return new MetricsFacade() {
      
      @Override
      public void updateTimer(String name, long duration, TimeUnit unit) {
        LOG.info("updateTimer name:{}, duration:{}, unit:{}", Arrays.asList(name, duration, unit));
      }
      
      @Override
      public void updateHistogram(String name, long value) {
        LOG.info("updateHistogram name:{}, value:{}", name, value);
      }
      
      @Override
      public void markMeter(String name, long increment) {
        LOG.info("markMeter name:{}, increment:{}", name, increment);
      }
    };
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  /** Helper that counts the number of solrj requests */
  private static final class CountingSolrServer extends SolrServer {
    
    private final SolrServer solrServer;
    private long numRequests = 0;
    
    public CountingSolrServer(SolrServer solrServer) {
      this.solrServer = solrServer;
    }
   
    public long getNumRequests() {
      return numRequests;
    }
    
    @Override
    public NamedList<Object> request(final SolrRequest request) throws SolrServerException, IOException {
      numRequests++;
      return solrServer.request(request);
    }
    
    @Override
    public DocumentObjectBinder getBinder() {
      return solrServer.getBinder();
    }

    @Override
    public void shutdown() {
      // NOP necessary for testing with EmbeddedSolrServer
      // solrServer.shutdown();
    }
  }
  
    
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  /**
   * Test helper that simulates an erratic SolrServer. Injects exceptions on the first N solrj
   * requests and succeeds on requests thereafter.
   */
  private static final class FailingSolrServer extends SolrServer {
    
    private final SolrServer solrServer;
    private long numRequests = 0;
    private long numInjectedFailures = 0;
    
    public static final int SUCCESS = 5;
    
    public FailingSolrServer(SolrServer solrServer) {
      this.solrServer = solrServer;
    }
   
    public long getNumRequests() {
      return numRequests;
    }
    
    public long getNumInjectedFailures() {
      return numInjectedFailures;
    }

    public void reset() {
      numRequests = 0;
      numInjectedFailures = 0;
    }

    @Override
    public NamedList<Object> request(final SolrRequest request) throws SolrServerException, IOException {
      if (++numRequests < SUCCESS) {
        numInjectedFailures++;
        if (numRequests % 2 == 0) {
          throw new InjectedSolrServerException("Injected failure");
        }
        try {
          throw new InjectedSolrServerException("Nested injected failure");
        } catch (InjectedSolrServerException e) {
          throw new InjectedSolrServerException("Injected failure", e); 
        }
      }
      return solrServer.request(request);
    }
    
    @Override
    public DocumentObjectBinder getBinder() {
      return solrServer.getBinder();
    }

    @Override
    public void shutdown() {
      // NOP necessary for testing with EmbeddedSolrServer
      // solrServer.shutdown();
    }

    
    ///////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    ///////////////////////////////////////////////////////////////////////////////
    public static final class InjectedSolrServerException extends SolrServerException {

      public InjectedSolrServerException(String message) {
        super(message);        
      }
      
      public InjectedSolrServerException(String message, Throwable cause) {
        super(message, cause);        
      }
      
    }
  }
  
}
