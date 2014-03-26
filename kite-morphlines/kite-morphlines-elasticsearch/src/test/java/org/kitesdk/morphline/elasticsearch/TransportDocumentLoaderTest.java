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
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.BytesStream;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import static org.mockito.Matchers.anyString;
import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class TransportDocumentLoaderTest {

  private TransportDocumentLoader fixture;

  @Mock
  private Client elasticSearchClient;

  @Mock
  private BulkRequestBuilder bulkRequestBuilder;

  @Mock
  private IndexRequestBuilder indexRequestBuilder;

  @Mock
  private BytesReference document;

  @Before
  public void setUp() throws IOException {
    initMocks(this);
    BytesStream bytesStream = mock(BytesStream.class);
    when(elasticSearchClient.prepareIndex(anyString(), anyString()))
            .thenReturn(indexRequestBuilder);
    when(indexRequestBuilder.setSource(document)).thenReturn(indexRequestBuilder);

    fixture = new TransportDocumentLoader(elasticSearchClient);
    fixture.setBulkRequestBuilder(bulkRequestBuilder);
  }

  @Test
  public void shouldAddNewEventWithoutTTL() throws Exception {
    fixture.addDocument(document, "foo", "bar_type", -1);
    verify(indexRequestBuilder).setSource(document);
    verify(bulkRequestBuilder).add(indexRequestBuilder);
  }

  @Test
  public void shouldAddNewEventWithTTL() throws Exception {
    fixture.addDocument(document, "foo", "bar_type", 10);
    verify(indexRequestBuilder).setTTL(10);
    verify(indexRequestBuilder).setSource(document);
  }

  @Test
  public void shouldExecuteBulkRequestBuilder() throws Exception {
    ListenableActionFuture<BulkResponse> action = mock(ListenableActionFuture.class);
    BulkResponse response = mock(BulkResponse.class);
    when(bulkRequestBuilder.execute()).thenReturn(action);
    when(action.actionGet()).thenReturn(response);
    when(response.hasFailures()).thenReturn(false);

    fixture.addDocument(document, "foo", "bar_type", 10);
    fixture.commitTransaction();
    verify(bulkRequestBuilder).execute();
  }

  @Test(expected = MorphlineRuntimeException.class)
  public void shouldThrowExceptionOnExecuteFailed() throws Exception {
    ListenableActionFuture<BulkResponse> action = mock(ListenableActionFuture.class);
    BulkResponse response = mock(BulkResponse.class);
    when(bulkRequestBuilder.execute()).thenReturn(action);
    when(action.actionGet()).thenReturn(response);
    when(response.hasFailures()).thenReturn(true);

    fixture.addDocument(document, "foo", "bar_type", 10);
    fixture.commitTransaction();
  }
}
