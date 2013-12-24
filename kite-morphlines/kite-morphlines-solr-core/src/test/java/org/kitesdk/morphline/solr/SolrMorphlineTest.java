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
package org.kitesdk.morphline.solr;

import java.io.File;
import java.util.Arrays;

import org.junit.Test;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Notifications;

public class SolrMorphlineTest extends AbstractSolrMorphlineTest {

  @Test
  public void testLoadSchema() throws Exception {
    SolrLocator locator = new SolrLocator(new MorphlineContext.Builder().build());
    locator.setCollectionName("collection1");
    locator.setSolrHomeDir("solr" + File.separator + "collection1");
    assertNotNull(locator.getIndexSchema());
  }
  
  @Test
  public void testLoadSolrBasic() throws Exception {
    //System.setProperty("ENV_SOLR_HOME", testSolrHome + File.separator + "collection1");
    morphline = createMorphline("test-morphlines" + File.separator + "loadSolrBasic");    
    //System.clearProperty("ENV_SOLR_HOME");
    Record record = new Record();
    record.put(Fields.ID, "id0");
    record.put("first_name", "Nadja"); // will be sanitized
    startSession();
    Notifications.notifyBeginTransaction(morphline);
    assertTrue(morphline.process(record));
    assertEquals(1, collector.getNumStartEvents());
    Notifications.notifyCommitTransaction(morphline);
    Record expected = new Record();
    expected.put(Fields.ID, "id0");
    assertEquals(Arrays.asList(expected), collector.getRecords());
    assertEquals(1, queryResultSetSize("*:*"));
    Notifications.notifyRollbackTransaction(morphline);
    Notifications.notifyShutdown(morphline);
  }
    
  @Test
  public void testTokenizeText() throws Exception {
    morphline = createMorphline("test-morphlines" + File.separator + "tokenizeText");
    for (int i = 0; i < 3; i++) {
      Record record = new Record();
      record.put(Fields.MESSAGE, "Hello World!");
      record.put(Fields.MESSAGE, "\nFoo@Bar.com #%()123");
      Record expected = record.copy();
      expected.getFields().putAll("tokens", Arrays.asList("hello", "world", "foo", "bar.com", "123"));
      collector.reset();
      startSession();
      Notifications.notifyBeginTransaction(morphline);
      assertTrue(morphline.process(record));
      assertEquals(1, collector.getNumStartEvents());
      Notifications.notifyCommitTransaction(morphline);
      assertEquals(expected, collector.getFirstRecord());
    }
  }
    
}
