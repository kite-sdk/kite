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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestDocumentLoader implements DocumentLoader {

  private static final String INDEX_OPERATION_NAME = "index";
  private static final String INDEX_PARAM = "_index";
  private static final String TYPE_PARAM = "_type";
  private static final String TTL_PARAM = "_ttl";
  private static final String BULK_ENDPOINT = "_bulk";

  private static final Logger LOGGER = LoggerFactory.getLogger(RestDocumentLoader.class);

  private final RoundRobinList<String> serversList;

  private StringBuilder bulkBuilder;
  private HttpClient httpClient;

  public RestDocumentLoader() {
    serversList = new RoundRobinList<String>(Arrays.asList("http://localhost:9200"));
  }

  public RestDocumentLoader(Collection<String> hostNames) {
    this(hostNames, new DefaultHttpClient());
  }

  public RestDocumentLoader(Collection<String> hostNames, HttpClient httpClient) {
    List<String> hosts = new LinkedList<String>();
    for (String hostName : hostNames) {
      if (!hostName.contains("http://") && !hostName.contains("https://")) {
        hosts.add("http://" + hostName);
      }
    }

    serversList = new RoundRobinList<String>(hosts);
    bulkBuilder = new StringBuilder();
    this.httpClient = httpClient;
  }

  @Override
  public void beginTransaction() throws IOException {
    bulkBuilder = new StringBuilder();
  }

  @Override
  public void commitTransaction() throws Exception {
    int statusCode = 0, triesCount = 0;
    HttpResponse response = null;
    LOGGER.info("Sending bulk request to elasticsearch cluster");

    String entity;
    synchronized (bulkBuilder) {
      entity = bulkBuilder.toString();
      bulkBuilder = new StringBuilder();
    }

    while (statusCode != HttpStatus.SC_OK && triesCount < serversList.size()) {
      triesCount++;
      String host = serversList.get();
      String url = host + "/" + BULK_ENDPOINT;
      HttpPost httpRequest = new HttpPost(url);
      httpRequest.setEntity(new StringEntity(entity));
      response = httpClient.execute(httpRequest);
      statusCode = response.getStatusLine().getStatusCode();
      LOGGER.info("Status code from elasticsearch: " + statusCode);
      if (response.getEntity() != null) {
        LOGGER.debug("Status message from elasticsearch: " + EntityUtils.toString(response.getEntity(), "UTF-8"));
      }
    }

    if (statusCode != HttpStatus.SC_OK) {
      if (response.getEntity() != null) {
        throw new MorphlineRuntimeException(EntityUtils.toString(response.getEntity(), "UTF-8"));
      } else {
        throw new MorphlineRuntimeException("Elasticsearch status code was: " + statusCode);
      }
    }
  }

  @Override
  public void rollbackTransaction() throws IOException {
    bulkBuilder = new StringBuilder();
  }

  @Override
  public void addDocument(BytesReference document, String index, String indexType, int ttlMs) throws Exception {
    Map<String, Map<String, String>> parameters = new HashMap<String, Map<String, String>>();
    Map<String, String> indexParameters = new HashMap<String, String>();
    indexParameters.put(INDEX_PARAM, index);
    indexParameters.put(TYPE_PARAM, indexType);
    if (ttlMs > 0) {
      indexParameters.put(TTL_PARAM, Long.toString(ttlMs));
    }
    parameters.put(INDEX_OPERATION_NAME, indexParameters);

    XContentBuilder documentBuilder = jsonBuilder().value(parameters);
    synchronized (bulkBuilder) {
      bulkBuilder.append(documentBuilder.bytes().toUtf8());
      bulkBuilder.append("\n");
      bulkBuilder.append(document.toBytesArray().toUtf8());
      bulkBuilder.append("\n");
    }
  }

  @Override
  public void shutdown() throws Exception {
    bulkBuilder = null;
  }

}
