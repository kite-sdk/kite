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
package org.kitesdk.morphline.saxon;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;
import org.kitesdk.morphline.api.AbstractMorphlineTest;
import org.kitesdk.morphline.api.Collector;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Notifications;
import org.kitesdk.morphline.stdlib.PipeBuilder;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;
import com.typesafe.config.Config;

public class TagsoupMorphlineTest extends AbstractMorphlineTest {

  @Test
  public void testConvertHTML() throws Exception {
    morphline = createMorphline("test-morphlines/convertHTML");    
    InputStream in = new FileInputStream(new File(RESOURCES_DIR + "/test-documents/helloworld.html"));
    Record record = new Record();
    record.put("id", "123");
    record.put(Fields.ATTACHMENT_BODY, in);
    String expected = Files.toString(new File(RESOURCES_DIR + "/test-documents/convertHTML-expected-output.xml"), Charsets.UTF_8);
    processAndVerifySuccess(record, 
        ImmutableMultimap.of("id", "123", Fields.MESSAGE, expected)
        );    
    in.close();
  }  

  @Test
  public void testConvertHTMLBlog() throws Exception {
    morphline = createMorphline("test-morphlines/convertHTML");    
    InputStream in = new FileInputStream(new File(RESOURCES_DIR + "/test-documents/blog.html"));
    Record record = new Record();
    record.put("id", "123");
    record.put(Fields.ATTACHMENT_BODY, in);
    String expected = Files.toString(new File(RESOURCES_DIR + "/test-documents/convertHTMLBlog-expected-output.xml"), Charsets.UTF_8);
    processAndVerifySuccess(record, 
        ImmutableMultimap.of("id", "123", Fields.MESSAGE, expected)
        );    
    in.close();
  }  

  @Test
  public void testConvertHTMLBlogThenRunXSQLT() throws Exception {
    morphline = createMorphline("test-morphlines/convertHTMLBlogThenRunXSLT");    
    byte[] bytes = Files.toByteArray(new File(RESOURCES_DIR + "/test-documents/blog.html"));
    Record record = new Record();
    record.put("id", "123");
    record.put(Fields.ATTACHMENT_BODY, bytes);
    for (int i = 0; i < 3; i++) {
      assertTrue(morphline.process(record.copy())); // TODO check details
    }
  }  

  @Test
  public void testConvertHTMLAndExtractLinks() throws Exception {
    morphline = createMorphline("test-morphlines/convertHTMLandExtractLinks");    
    InputStream in = new FileInputStream(new File(RESOURCES_DIR + "/test-documents/helloworld.html"));
    Record record = new Record();
    record.put("id", "123");
    record.put(Fields.ATTACHMENT_BODY, in);
    processAndVerifySuccess(record, 
        ImmutableMultimap.of("id", "123", "a", "Visit Foo!", "myhref", "http://www.foo.com/", "mytarget", "_foo"),
        ImmutableMultimap.of("id", "123", "a", "Visit Bar!", "myhref", "http://www.bar.com/")
        );    
    in.close();
  }  

  private void processAndVerifySuccess(Record input, Multimap... expectedMaps) {
    collector.reset();
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(input));    
    Iterator<Record> iter = collector.getRecords().iterator();
    int i = 0;
    for (Multimap expected : expectedMaps) {
      //System.out.println("iter" + i);
      assertTrue(iter.hasNext());
      assertEquals(expected, iter.next().getFields());
      i++;
    }    
    assertFalse(iter.hasNext());
  }
  
  private void processAndVerifySuccess(Command myMorphline, Collector myCollector, Record input, Multimap... expectedMaps) {
    myCollector.reset();
    Notifications.notifyStartSession(myMorphline);
    assertEquals(1, myCollector.getNumStartEvents());
    assertTrue(myMorphline.process(input));    
    Iterator<Record> iter = myCollector.getRecords().iterator();
    int i = 0;
    for (Multimap expected : expectedMaps) {
      //System.out.println("iter" + i);
      assertTrue(iter.hasNext());
      assertEquals(expected, iter.next().getFields());
      i++;
    }    
    assertFalse(iter.hasNext());
  }
  
  @Test
  @Ignore
  public void testMultiThreading() throws Exception {
    byte[] bytes = Files.toByteArray(new File(RESOURCES_DIR + "/test-documents/helloworld.html"));
    String expected = Files.toString(new File(RESOURCES_DIR + "/test-documents/convertHTML-expected-output.xml"), Charsets.UTF_8);    
    testMultiThreading(bytes, expected);
  }
  
  @Test
  @Ignore
  public void testMultiThreadingBlog() throws Exception {
    byte[] bytes = Files.toByteArray(new File(RESOURCES_DIR + "/test-documents/blog.html"));
    String expected = Files.toString(new File(RESOURCES_DIR + "/test-documents/convertHTMLBlog-expected-output.xml"), Charsets.UTF_8);
    testMultiThreading(bytes, expected);
  }
  
  private void testMultiThreading(final byte[] bytes, final String expected) throws Exception {
    Logger logger = Logger.getLogger(MorphlineContext.class);
    Level oldLevel = logger.getLevel();
    logger.setLevel(Level.WARN); // avoid spitting out tons of log messages; will revert at end of method
    final AtomicLong totalIters = new AtomicLong(0);
    final CountDownLatch hasException = new CountDownLatch(1);
    try {
      int numThreads = 16;
      final int durationMillis = 2000;
      Thread[] threads = new Thread[numThreads];
      for (int t = 0; t < numThreads; t++) {
        threads[t] = new Thread(new Runnable() {
          public void run() {
            try {
              int iters = 0;
              MorphlineContext ctx = new MorphlineContext.Builder().build();
              Config config = parse("test-morphlines/convertHTML");
              Collector myCollector = new Collector();
              Command myMorphline = new PipeBuilder().build(config, null, myCollector, ctx);
              
              long start = System.currentTimeMillis();
              while (System.currentTimeMillis() < start + durationMillis) {
                Record record = new Record();
                record.put("id", "123");
                record.put(Fields.ATTACHMENT_BODY, bytes);
                for (int i = 0; i < 3; i++) {
                  processAndVerifySuccess(myMorphline, myCollector, record.copy(), 
                      ImmutableMultimap.of("id", "123", Fields.MESSAGE, expected)
                      );    
                }
                iters++;
                //break;
              }
              totalIters.addAndGet(iters);
            } catch (Exception e) {
              hasException.countDown();
              throw new RuntimeException(e);
            }    
          }        
        });
      }
      
      for (int t = 0; t < numThreads; t++) {
        threads[t].start();
      }
      for (int t = 0; t < numThreads; t++) {
        threads[t].join();
      }
    } finally {
      logger.setLevel(oldLevel);      
    }
    System.out.println("tagsoupTotalIters=" + totalIters);
    assertTrue(hasException.getCount() > 0);
  }  
  
}
