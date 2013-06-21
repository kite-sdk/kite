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
package com.cloudera.cdk.morphline.twitter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;

import com.cloudera.cdk.morphline.api.AbstractMorphlineTest;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.Fields;

public class ReadJsonTweetsTest extends AbstractMorphlineTest {
  
  @Test
  public void testReadJsonTweets() throws Exception {
    morphline = createMorphline("test-morphlines/readJsonTweets");    
    Record record = new Record();
    record.put(Fields.ATTACHMENT_BODY, getInputStream("non-length-delimited-20130430-234145-tweets.json.gz"));
    record.put(Fields.ATTACHMENT_NAME, "non-length-delimited-20130430-234145-tweets.json.gz");
    startSession();
    assertTrue(morphline.process(record));
    assertEquals(1, collector.getNumStartEvents());
    Iterator<Record> iter = collector.getRecords().iterator();
    assertEquals(Arrays.asList("1985-09-04T18:01:01Z"), iter.next().get("created_at"));
    assertEquals(Arrays.asList("1985-09-04T19:14:34Z"), iter.next().get("created_at"));
    assertFalse(iter.hasNext());
  }
    
  @Test
  public void testReadJsonTweetsLengthDelimited() throws Exception {
    morphline = createMorphline("test-morphlines/readJsonTweetsLengthDelimited");    
    Record record = new Record();    
    record.put(Fields.ATTACHMENT_BODY, getInputStream("sample-statuses-20120906-141433"));
    startSession();
    assertTrue(morphline.process(record));
    assertEquals(1, collector.getNumStartEvents());
    Iterator<Record> iter = collector.getRecords().iterator();
    assertEquals(Arrays.asList("1985-09-04T18:01:01Z"), iter.next().get("created_at"));
    assertEquals(Arrays.asList("1985-09-04T19:14:34Z"), iter.next().get("created_at"));
    assertFalse(iter.hasNext());
  }
  
  private InputStream getInputStream(String file) throws FileNotFoundException {
    return new FileInputStream(new File(RESOURCES_DIR + "/test-documents/" + file));
  }
    
}
