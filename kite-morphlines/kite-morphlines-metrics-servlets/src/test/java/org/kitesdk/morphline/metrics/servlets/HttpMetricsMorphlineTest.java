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
package org.kitesdk.morphline.metrics.servlets;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.ConnectException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Iterator;

import org.junit.Test;
import org.kitesdk.morphline.api.AbstractMorphlineTest;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Notifications;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.CharStreams;

public class HttpMetricsMorphlineTest extends AbstractMorphlineTest {

  @Test
  public void testBasic() throws Exception {
    morphline = createMorphline("test-morphlines/startReportingMetricsToHTTP");    
    
    Record record = new Record();
    String msg = "foo";
    record.put(Fields.MESSAGE, msg);
    Record expected = new Record();
    expected.put(Fields.MESSAGE, msg);
    processAndVerifySuccess(record, expected);

    if ("true".equals(System.getProperty("HttpMetricsMorphlineTest.isDemo"))) {
      // wait forever so user can browse to http://localhost:8080/ and interactively explore the features
      Thread.sleep(Long.MAX_VALUE); 
    }

    verifyServing(8080);
    verifyServing(8081);
    verifyShutdown(8080);
    verifyShutdown(8081);
  }
  
  // verify jetty is up and responding correctly
  private void verifyServing(int port) throws IOException {
    String response = httpGet(port, "");
    assertTrue(response.contains("Ping"));
    assertTrue(response.contains("Metrics"));
    assertTrue(response.contains("Healthcheck"));
    
    response = httpGet(port, "/ping");
    assertEquals("pong", response.trim());
    
    response = httpGet(port, "/threads");
    assertTrue(response.startsWith("main id="));
    
    response = httpGet(port, "/healthcheck");
    assertTrue(response.startsWith("{\"deadlocks\":{\"healthy\":true}}"));
    
    response = httpGet(port, "/metrics");
    assertTrue(response.startsWith("{\"version\":"));    
    Iterator iter = new ObjectMapper().reader(JsonNode.class).readValues(response);
    JsonNode node = (JsonNode) iter.next();
    assertEquals(1, node.get("counters").get("myMetrics.myCounter").get("count").asInt());
    assertEquals(2, node.get("meters").get("morphline.logWarn.numProcessCalls").get("count").asInt());
    assertEquals(3, node.get("meters").get("morphline.logDebug.numProcessCalls").get("count").asInt());    
    assertTrue(node.get("gauges").get("jvm.memory.heap.used").get("value").asInt() > 0);
    
    assertFalse(iter.hasNext());    
  }
  
  private void verifyShutdown(int port) throws IOException {
    for (int i = 0; i < 2; i++) {
      Notifications.notifyShutdown(morphline);
      try {
        httpGet(port, "");
        fail();
      } catch (ConnectException e) {
        ; // expected
      }
    }    
  }
  
  private String httpGet(int port, String path) throws IOException {
    URL url = new URL("http://localhost:" + port + path);
    URLConnection conn = url.openConnection();
    Reader reader = new InputStreamReader(conn.getInputStream());
    String response = CharStreams.toString(reader);
    reader.close();
    return response;
  }

  private void processAndVerifySuccess(Record input, Record expected) {
    processAndVerifySuccess(input, expected, true);
  }

  private void processAndVerifySuccess(Record input, Record expected, boolean isSame) {
    collector.reset();
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(input));
    assertEquals(expected, collector.getFirstRecord());
    if (isSame) {
      assertSame(input, collector.getFirstRecord());    
    } else {
      assertNotSame(input, collector.getFirstRecord());    
    }
  }

}
