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
package org.kitesdk.morphline.json;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;
import org.kitesdk.morphline.api.AbstractMorphlineTest;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

public class JsonMorphlineTest extends AbstractMorphlineTest {

  @Test
  public void testReadJson() throws Exception {
    morphline = createMorphline("test-morphlines/readJson");    
    for (int j = 0; j < 3; j++) { // also test reuse of objects and low level avro buffers
      InputStream in = new FileInputStream(new File(RESOURCES_DIR + "/test-documents/stream.json"));
      Record record = new Record();
      record.put(Fields.ATTACHMENT_BODY, in);
      
      collector.reset();
      startSession();
      assertEquals(1, collector.getNumStartEvents());
      assertTrue(morphline.process(record));    
      Iterator<Record> iter = collector.getRecords().iterator();
      
      assertTrue(iter.hasNext());
      JsonNode node = (JsonNode) iter.next().getFirstValue(Fields.ATTACHMENT_BODY);
      assertEquals("foo", node.get("firstObject").asText());
      assertTrue(node.isObject());
      assertEquals(1, node.size());
      
      assertTrue(iter.hasNext());
      node = (JsonNode) iter.next().getFirstValue(Fields.ATTACHMENT_BODY);
      assertEquals("bar", node.get("secondObject").asText());
      assertTrue(node.isObject());
      assertEquals(1, node.size());
      
      assertFalse(iter.hasNext());
      in.close();
    }
  }
  
  @Test
  public void testReadJsonWithMap() throws Exception {
    morphline = createMorphline("test-morphlines/readJsonWithMap");    
    for (int j = 0; j < 3; j++) { // also test reuse of objects and low level avro buffers
      InputStream in = new FileInputStream(new File(RESOURCES_DIR + "/test-documents/stream.json"));
      Record record = new Record();
      record.put(Fields.ATTACHMENT_BODY, in);
      
      collector.reset();
      startSession();
      assertEquals(1, collector.getNumStartEvents());
      assertTrue(morphline.process(record));    
      Iterator<Record> iter = collector.getRecords().iterator();
      
      assertTrue(iter.hasNext());
      Map node = (Map) iter.next().getFirstValue(Fields.ATTACHMENT_BODY);
      assertEquals(ImmutableMap.of("firstObject", "foo"), node);
      
      assertTrue(iter.hasNext());
      node = (Map) iter.next().getFirstValue(Fields.ATTACHMENT_BODY);
      assertEquals(ImmutableMap.of("secondObject", "bar"), node);
      
      assertFalse(iter.hasNext());
      in.close();
    }
  }
  
  @Test
  public void testExtractJsonPaths() throws Exception {
    morphline = createMorphline("test-morphlines/extractJsonPaths");    
    File file = new File(RESOURCES_DIR + "/test-documents/arrays.json");
    InputStream in = new FileInputStream(file);
    Record record = new Record();
    record.put(Fields.ATTACHMENT_BODY, in);
    
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));    
    
    assertEquals(1, collector.getRecords().size());
    JsonNode rootNode = (JsonNode) new ObjectMapper().reader(JsonNode.class).readValues(file).next();
    assertTrue(rootNode.get("price").isArray());
    List<JsonNode> expected = Arrays.asList(rootNode.get("price"));
    assertEquals(1, collector.getRecords().size());
    assertEquals(expected, collector.getFirstRecord().get("/price"));
    assertEquals(expected, collector.getFirstRecord().get("/price/[]"));
    assertEquals(Arrays.asList(), collector.getFirstRecord().get("/unknownField"));

    in.close();
  }
  
  @Test
  public void testExtractJsonPathsFlattened() throws Exception {
    morphline = createMorphline("test-morphlines/extractJsonPathsFlattened");    
    File file = new File(RESOURCES_DIR + "/test-documents/arrays.json");
    InputStream in = new FileInputStream(file);
    Record record = new Record();
    record.put(Fields.ATTACHMENT_BODY, in);
    
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));    
    
    assertEquals(1, collector.getRecords().size());
    List expected = Arrays.asList(1, 2, 3, 4, 5, 10, 20, 100, 200);
    assertEquals(1, collector.getRecords().size());
    assertEquals(expected, collector.getFirstRecord().get("/price"));
    assertEquals(expected, collector.getFirstRecord().get("/price/[]"));
    assertEquals(Arrays.asList(), collector.getFirstRecord().get("/unknownField"));

    in.close();
  }

  @Test
  public void testComplexDocuments() throws Exception {
    morphline = createMorphline("test-morphlines/extractJsonPaths");    
    File file = new File(RESOURCES_DIR + "/test-documents/complex.json");
    InputStream in = new FileInputStream(file);
    Record record = new Record();
    record.put(Fields.ATTACHMENT_BODY, in);
    
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));    
    
    assertEquals(1, collector.getRecords().size());
    JsonNode rootNode = (JsonNode) new ObjectMapper().reader(JsonNode.class).readValues(file).next();
    JsonNodeFactory factory = new JsonNodeFactory(false);

    assertEquals(Arrays.asList(10), collector.getFirstRecord().get("/docId"));
    assertEquals(Arrays.asList(rootNode.get("links")), collector.getFirstRecord().get("/links"));
    
    assertEquals(Arrays.asList(factory.arrayNode()), collector.getFirstRecord().get("/links/backward"));
    assertEquals(factory.arrayNode(), rootNode.get("links").get("backward"));
    
    List expected = Arrays.asList(factory.arrayNode().add(20).add(40).add(60).add(true).add(false).add(32767).add(2147483647).add(9223372036854775807L).add(1.23).add(1.7976931348623157E308));
    assertEquals(expected, collector.getFirstRecord().get("/links/forward"));
    assertEquals(expected, collector.getFirstRecord().get("/links/forward/[]"));
    assertEquals(expected, collector.getFirstRecord().get("/links/forward[]"));
    assertEquals(Arrays.asList(rootNode.get("name")), collector.getFirstRecord().get("/name"));
    assertEquals(Arrays.asList("en-us", "en", "en-gb"), collector.getFirstRecord().get("/name/[]/language/[]/code"));
    assertEquals(Arrays.asList("en-us", "en", "en-gb"), collector.getFirstRecord().get("/name[]/language[]/code"));
    assertEquals(Arrays.asList("us", "gb"), collector.getFirstRecord().get("/name/[]/language/[]/country"));
    assertEquals(Arrays.asList("us", "gb"), collector.getFirstRecord().get("/name[]/language[]/country"));
    assertEquals(Arrays.asList(), collector.getFirstRecord().get("/unknownField"));
    assertEquals(Arrays.asList(true), collector.getFirstRecord().get("/links/bool"));
    assertEquals(Arrays.asList(32767), collector.getFirstRecord().get("/links/short"));
    assertEquals(Arrays.asList(2147483647), collector.getFirstRecord().get("/links/int"));
    assertEquals(Arrays.asList(9223372036854775807L), collector.getFirstRecord().get("/links/long"));
    assertEquals(Arrays.asList(1.7976931348623157E308), collector.getFirstRecord().get("/links/double"));    

    in.close();    
  }

  @Test
  public void testExtractJsonPathsComplexFlattened() throws Exception {
    morphline = createMorphline("test-morphlines/extractJsonPathsFlattened");    
    File file = new File(RESOURCES_DIR + "/test-documents/complex.json");
    InputStream in = new FileInputStream(file);
    Record record = new Record();
    record.put(Fields.ATTACHMENT_BODY, in);
    
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));        
    assertEquals(1, collector.getRecords().size());
    
    List expected = Arrays.asList(20, 40, 60, true, false, 32767, 2147483647, 9223372036854775807L, 
        1.23, 1.7976931348623157E308, 
        true, 32767, 2147483647, 9223372036854775807L, 1.7976931348623157E308
        );
    assertEquals(expected, collector.getFirstRecord().get("/links"));    
    expected = Arrays.asList(20, 40, 60, true, false, 32767, 2147483647, 9223372036854775807L, 
        1.23, 1.7976931348623157E308);
    assertEquals(expected, collector.getFirstRecord().get("/links/forward"));

    expected = Arrays.asList("en-us", "us", "en", "http://A", "http://B", "en-gb", "gb");
    assertEquals(expected, collector.getFirstRecord().get("/name"));

    assertEquals(Arrays.asList(), collector.getFirstRecord().get("/unknownField"));

    assertEquals(Arrays.asList(true), collector.getFirstRecord().get("/links/bool"));
    assertEquals(Arrays.asList(32767), collector.getFirstRecord().get("/links/short"));
    assertEquals(Arrays.asList(2147483647), collector.getFirstRecord().get("/links/int"));
    assertEquals(Arrays.asList(9223372036854775807L), collector.getFirstRecord().get("/links/long"));
    assertEquals(Arrays.asList(1.7976931348623157E308), collector.getFirstRecord().get("/links/double"));    

    in.close();
  }

  @Test
  @Ignore
  public void benchmarkJson() throws Exception {
    String morphlineConfigFile = "test-morphlines/readJson";
    long durationSecs = 10;
    //File file = new File(RESOURCES_DIR + "/test-documents/stream.json");
    File file = new File(RESOURCES_DIR + "/test-documents/sample-statuses-20120906-141433.json");
    System.out.println("Now benchmarking " + morphlineConfigFile + " ...");
    morphline = createMorphline(morphlineConfigFile);    
    byte[] bytes = Files.toByteArray(file);
    long start = System.currentTimeMillis();
    long duration = durationSecs * 1000;
    int iters = 0; 
    while (System.currentTimeMillis() < start + duration) {
      Record record = new Record();
      record.put(Fields.ATTACHMENT_BODY, bytes);      
      collector.reset();
      startSession();
      assertEquals(1, collector.getNumStartEvents());
      assertTrue(morphline.process(record));    
      iters++;
    }
    float secs = (System.currentTimeMillis() - start) / 1000.0f;
    System.out.println("Results: iters=" + iters + ", took[secs]=" + secs + ", iters/secs=" + (iters/secs));
  }  

}
