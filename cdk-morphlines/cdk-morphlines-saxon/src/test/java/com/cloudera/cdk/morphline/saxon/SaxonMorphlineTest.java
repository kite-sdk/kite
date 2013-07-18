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
package com.cloudera.cdk.morphline.saxon;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Iterator;

import org.junit.Ignore;
import org.junit.Test;

import com.cloudera.cdk.morphline.api.AbstractMorphlineTest;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.Fields;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;

public class SaxonMorphlineTest extends AbstractMorphlineTest {

  @Test
  public void testXQueryTweetTexts() throws Exception {
    morphline = createMorphline("test-morphlines/xquery-tweet-texts");    
    InputStream in = new FileInputStream(new File(RESOURCES_DIR + "/test-documents/sample-statuses-20120906-141433.xml"));
    Record record = new Record();
    record.put("id", "123");
    record.put(Fields.ATTACHMENT_BODY, in);
    processAndVerifySuccess(record, 
        ImmutableMultimap.of("id", "123", "text", "sample tweet one"),
        ImmutableMultimap.of("id", "123", "text", "sample tweet two")
        );    
    in.close();
  }  

  @Test
  public void testXQueryTweetUsers() throws Exception {
    morphline = createMorphline("test-morphlines/xquery-tweet-users");    
    InputStream in = new FileInputStream(new File(RESOURCES_DIR + "/test-documents/sample-statuses-20120906-141433.xml"));
    Record record = new Record();
    record.put(Fields.ATTACHMENT_BODY, in);
    processAndVerifySuccess(record, 
        ImmutableMultimap.of("followers_count", "111", "id", "11111112", "screen_name", "fake_user1", "statuses_count", "11111", "greeting", "hello world"),
        ImmutableMultimap.of("followers_count", "222", "id", "222223", "screen_name", "fake_user2", "statuses_count", "22222", "greeting", "hello world")
        );    
    in.close();
  }  

  @Test
  public void testXQueryAtomFeeds() throws Exception {
    morphline = createMorphline("test-morphlines/xquery-atom-feeds");    
    InputStream in = new FileInputStream(new File(RESOURCES_DIR + "/test-documents/atom.xml"));
    Record record = new Record();
    record.put(Fields.ATTACHMENT_BODY, in);
    processAndVerifySuccess(record, 
        ImmutableMultimap
            .of("id",
                "tag:blogger.com,1999:blog-10832468.post-112136653221060965",
                "summary",
                "A Great Place To Pick Up Cast Iron Pan Or Circulon Cookware On The Web 		 		You don't have to wait to get the cast iron pan that is right for you.  Everything you need to know about cast iron pan is online.  All this came to me as I was looking out the window.  You decide what cast iron pan is right for you. It is so easy and fast!  Cast Iron Pan : Cast Iron Pan",
                "title", "Cast Iron Pan", "generator", "Blogger"),
        ImmutableMultimap
            .of("id",
                "tag:blogger.com,1999:blog-10832468.post-112135176551133849",
                "summary",
                "A Great Place To Shop For Soapstone Cookware Or Roll Pan Cheap 		 		The best part about it is, it's so easy.  You will always have your soapstone cookware.  Go over to Google and type in soapstone cookware in the search form.  soapstone cookware popped right out in front of me.  Just try a single search for soapstone cookware.  Soapstone Cookware : Soapstone Cookware",
                "title", "Soapstone Cookware", "generator", "Blogger"),
        ImmutableMultimap
            .of("id",
                "tag:blogger.com,1999:blog-10832468.post-112133988275976426",
                "summary",
                "The Best Place To Obtain Air Core Cookware Set Or Cookware Stores On The Internet 		 		There is no better way to get air core cookware set faster.  Everything you need to know about air core cookware set is online.  The internet is the place to find it.  This is not just local info, you literally have access to worldwide solutions for air core cookware set.  The online forms to get my air core",
                "title", "Air Core Cookware Set", "generator", "Blogger"));
    in.close();
  }  

  @Test
  public void testXQueryShakespeareSpeakers() throws Exception {
    morphline = createMorphline("test-morphlines/xquery-shakespeare-speakers");    
    InputStream in = new FileInputStream(new File(RESOURCES_DIR + "/test-documents/othello.xml"));
    Record record = new Record();
    record.put(Fields.ATTACHMENT_BODY, in);
    processAndVerifySuccess(record, 
        ImmutableMultimap.of("name", "OTHELLO", "frequency", "274"),
        ImmutableMultimap.of("name", "IAGO", "frequency", "272"),
        ImmutableMultimap.of("name", "DESDEMONA", "frequency", "165")
        );    
    in.close();
  }  

  @Test
  public void testXQueryAtomicValues() throws Exception {
    morphline = createMorphline("test-morphlines/xquery-atomic-values");    
    InputStream in = new FileInputStream(new File(RESOURCES_DIR + "/test-documents/sample-statuses-20120906-141433.xml"));
    Record record = new Record();
    record.put(Fields.ATTACHMENT_BODY, in);
    processAndVerifySuccess(record); 
    in.close();
  }  

  @Test
  public void testXsltIdentityHelloWorld() throws Exception {
    morphline = createMorphline("test-morphlines/xslt-helloworld-identity");    
    InputStream in = new FileInputStream(new File(RESOURCES_DIR + "/test-documents/helloworld.xml"));
    Record record = new Record();
    record.put(Fields.ATTACHMENT_BODY, in);
    processAndVerifySuccess(record, 
        ImmutableMultimap.of("description", "An XSLT Morphline", "welcome", "Hello, World!")
        );    
    in.close();
  }  

  @Test
  public void testXsltHelloWorldSequence() throws Exception {
    morphline = createMorphline("test-morphlines/xslt-helloworld-sequence");    
    InputStream in = new FileInputStream(new File(RESOURCES_DIR + "/test-documents/helloworld.xml"));
    Record record = new Record();
    record.put("id", "123");
    record.put(Fields.ATTACHMENT_BODY, in);
    processAndVerifySuccess(record, 
        ImmutableMultimap.of("id", "123", "attr", "foo", "HEAD", "title1", "BODY", "Hello, World!Paragraph1aParagraph1b"),
        ImmutableMultimap.of("id", "123", "HEAD", "title2", "BODY", "Hello, World!Paragraph2aParagraph2b")
        );    
    in.close();
  }  

  @Test
  @Ignore
  public void benchmarkSaxon() throws Exception {
    String morphlineConfigFile = "test-morphlines/xquery-tweet-texts";
    //String morphlineConfigFile = "test-morphlines/xslt-helloworld";
    //String morphlineConfigFile = "test-morphlines/convertHTML";
    long durationSecs = 20;
    File file = new File(RESOURCES_DIR + "/test-documents/sample-statuses-20120906-141433.xml");
    //File file = new File(RESOURCES_DIR + "/test-documents/helloworld.xml");
    //File file = new File(RESOURCES_DIR + "/test-documents/blog.html");
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
  
}
