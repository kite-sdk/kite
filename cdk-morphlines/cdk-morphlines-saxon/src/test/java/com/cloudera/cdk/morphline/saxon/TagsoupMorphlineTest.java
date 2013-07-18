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

import org.junit.Test;

import com.cloudera.cdk.morphline.api.AbstractMorphlineTest;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.Fields;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;

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
    morphline = createMorphline("test-morphlines/convertHTMLAndExtractLinks");    
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
  
}
