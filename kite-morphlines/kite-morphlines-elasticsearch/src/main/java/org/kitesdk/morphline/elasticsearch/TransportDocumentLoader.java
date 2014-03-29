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

package org.kitesdk.morphline.elasticsearch;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransportDocumentLoader implements DocumentLoader {

  public static final int DEFAULT_PORT = 9300;
  
  private static final Logger LOGGER = LoggerFactory.getLogger(TransportDocumentLoader.class);
  
  private Collection<InetSocketTransportAddress> serverAddresses;
  private BulkRequestBuilder bulkRequestBuilder;

  private Client client;

  public TransportDocumentLoader() {
    openLocalDiscoveryClient();
  }

  public TransportDocumentLoader(Client client) {
    this.client = client;
  }
  
  public TransportDocumentLoader(Collection<String> hostNames, String clusterName) {
    serverAddresses = new LinkedList<InetSocketTransportAddress>();
    for (String hostName : hostNames) {
      String[] hostPort = hostName.trim().split(":");
      String host = hostPort[0].trim();
      int port = hostPort.length == 2 ? Integer.parseInt(hostPort[1].trim()) : DEFAULT_PORT;
      serverAddresses.add(new InetSocketTransportAddress(host, port));
    }
    openClient(clusterName);
  }

  @VisibleForTesting
  public void setBulkRequestBuilder(BulkRequestBuilder bulkRequestBuilder) {
    this.bulkRequestBuilder = bulkRequestBuilder;
  }

  @Override
  public void beginTransaction() throws IOException {
    bulkRequestBuilder = client.prepareBulk();
  }

  @Override
  public void commitTransaction() throws Exception {
    try {
      LOGGER.debug("Sending bulk to elasticsearch cluster");
      BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
      if (bulkResponse.hasFailures()) {
        throw new MorphlineRuntimeException(bulkResponse.buildFailureMessage());
      }
    } finally {
      bulkRequestBuilder = client.prepareBulk();
    }
  }

  @Override
  public void rollbackTransaction() throws IOException {
    bulkRequestBuilder = null;
  }

  @Override
  public void addDocument(BytesReference document, String index, String indexType, int ttlMs) throws Exception {
    if (bulkRequestBuilder == null) {
      bulkRequestBuilder = client.prepareBulk();
    }

    IndexRequestBuilder indexRequestBuilder = null;
    indexRequestBuilder = client.prepareIndex(index, indexType).setSource(document);
    if (ttlMs > 0) {
      indexRequestBuilder.setTTL(ttlMs);
    }
    bulkRequestBuilder.add(indexRequestBuilder);
  }

  @Override
  public void shutdown() throws Exception {
    if (client != null) {
      client.close();
    }
    client = null;
  }

  /**
   * Open client to elaticsearch cluster
   *
   * @param clusterName
   */
  private void openClient(String clusterName) {
    Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build();

    TransportClient transportClient = new TransportClient(settings);
    for (InetSocketTransportAddress host : serverAddresses) {
      transportClient.addTransportAddress(host);
    }
    if (client != null) {
      client.close();
    }
    client = transportClient;
  }

  /*
   * FOR TESTING ONLY. Open local connection.
   */
  private void openLocalDiscoveryClient() {
    LOGGER.info("Using ElasticSearch AutoDiscovery mode");
    Node node = NodeBuilder.nodeBuilder().client(true).local(true).node();
    if (client != null) {
      client.close();
    }
    client = node.client();
  }
}
