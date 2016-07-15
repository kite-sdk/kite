/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Ignore;
import org.junit.Test;
import org.kitesdk.morphline.api.MorphlineContext;

/** Verify that the correct Solr Server is selected based on parameters given the locator */
public class SolrLocatorTest {

  private static final String RESOURCES_DIR = "target" + File.separator + "test-classes";
  
  @Test
  @Ignore
  public void testSelectsEmbeddedSolrServer() throws Exception {
    //Solr locator should select EmbeddedSolrServer only solrHome is specified
    SolrLocator solrLocator = new SolrLocator(new MorphlineContext.Builder().build());
    solrLocator.setSolrHomeDir(RESOURCES_DIR + "/solr");
    solrLocator.setCollectionName("collection1");
    SolrServerDocumentLoader documentLoader = (SolrServerDocumentLoader)solrLocator.getLoader();
    SolrClient solrServer = documentLoader.getSolrServer();
    assertTrue(solrServer instanceof EmbeddedSolrServer);
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "myval");
    solrServer.add(doc);
    solrServer.commit();
  }

}

