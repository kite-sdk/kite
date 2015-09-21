/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.morphline.solr;

import java.io.File;
import java.util.Collections;
import java.util.Iterator;

import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Notifications;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction.Action;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies.Consequence;

@ThreadLeakAction({Action.WARN})
@ThreadLeakLingering(linger = 0)
@ThreadLeakZombies(Consequence.CONTINUE)
@ThreadLeakScope(Scope.NONE)
@SuppressCodecs({"Lucene3x", "Lucene40"})
public class SolrMorphlineZkTest extends AbstractSolrMorphlineZkTest {
    
  @Override
  public void doTest() throws Exception {
    
    waitForRecoveriesToFinish(false);
    
    morphline = parse("test-morphlines" + File.separator + "loadSolrBasic");    
    Record record = new Record();
    record.put(Fields.ID, "id0-innsbruck");
    record.put("text", "mytext");
    record.put("user_screen_name", "foo");
    record.put("first_name", "Nadja"); // will be sanitized
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    Notifications.notifyBeginTransaction(morphline);
    assertTrue(morphline.process(record));

    record = new Record();
    record.put(Fields.ID, "id0-innsbruck");
    record.put("user_screen_name", Collections.singletonMap("set", "bar"));
    assertTrue(morphline.process(record));

    record = new Record();
    record.put(Fields.ID, "id1-innsbruck");
    record.put("text", "mytext1");
    record.put("user_screen_name", "foo1");
    record.put("first_name", "Nadja1"); // will be sanitized
    assertTrue(morphline.process(record));
    
    Record expected = new Record();
    expected.put(Fields.ID, "id0-innsbruck");
    expected.put("text", "mytext");
    expected.put("user_screen_name", "bar");
    
    Record expected2 = new Record();
    expected2.put(Fields.ID, "id1-innsbruck");
    expected2.put("text", "mytext1");
    expected2.put("user_screen_name", "foo1");
    
    commit();
    
    QueryResponse rsp = cloudClient.query(new SolrQuery("*:*").setRows(100000).addSort(Fields.ID, SolrQuery.ORDER.asc));
    //System.out.println(rsp);
    Iterator<SolrDocument> iter = rsp.getResults().iterator();
    assertEquals(expected.getFields(), next(iter));
    assertEquals(expected2.getFields(), next(iter));
    assertFalse(iter.hasNext());
    
    Notifications.notifyRollbackTransaction(morphline);
    Notifications.notifyShutdown(morphline);
    cloudClient.shutdown();
  }

}
