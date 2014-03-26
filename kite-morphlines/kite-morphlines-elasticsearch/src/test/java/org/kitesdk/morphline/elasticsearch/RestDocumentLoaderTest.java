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
import java.util.List;
import static junit.framework.Assert.assertEquals;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.mockito.ArgumentCaptor;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.isA;
import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class RestDocumentLoaderTest {

  private RestDocumentLoader fixture;

  @Mock
  private BytesReference document;

  @Mock
  private HttpClient httpClient;

  @Mock
  private HttpResponse httpResponse;

  @Mock
  private StatusLine httpStatus;

  @Mock
  private HttpEntity httpEntity;

  private static final String INDEX_NAME = "foo_index";
  private static final String INDEX_TYPE = "bar_type";
  private static final String MESSAGE_CONTENT = "{\"body\":\"test\"}";
  private static final String[] HOSTS = {"host1", "host2"};

  @Before
  public void setUp() throws IOException {
    initMocks(this);
    BytesArray bytesArrayContent = mock(BytesArray.class);
    when(bytesArrayContent.toUtf8()).thenReturn(MESSAGE_CONTENT);
    when(document.toBytesArray()).thenReturn(bytesArrayContent);
    fixture = new RestDocumentLoader(Arrays.asList(HOSTS), httpClient);
  }

  @Test
  public void shouldAddNewEventWithoutTTL() throws Exception {
    ArgumentCaptor<HttpPost> argument = ArgumentCaptor.forClass(HttpPost.class);

    when(httpStatus.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(httpResponse.getStatusLine()).thenReturn(httpStatus);
    when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(httpResponse);

    fixture.addDocument(document, INDEX_NAME, INDEX_TYPE, -1);
    fixture.commitTransaction();

    verify(httpClient).execute(isA(HttpUriRequest.class));
    verify(httpClient).execute(argument.capture());

    System.out.println(EntityUtils.toString(argument.getValue().getEntity()));
    System.out.println("{\"index\":{\"_type\":\"bar_type\",\"_index\":\"foo_index\"}}\n" + MESSAGE_CONTENT + "\n");

    assertEquals("http://host1/_bulk", argument.getValue().getURI().toString());
    assertEquals("{\"index\":{\"_type\":\"bar_type\",\"_index\":\"foo_index\"}}\n" + MESSAGE_CONTENT + "\n",
            EntityUtils.toString(argument.getValue().getEntity()));
  }

  @Test
  public void shouldAddNewEventWithTTL() throws Exception {
    ArgumentCaptor<HttpPost> argument = ArgumentCaptor.forClass(HttpPost.class);

    when(httpStatus.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(httpResponse.getStatusLine()).thenReturn(httpStatus);
    when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(httpResponse);

    fixture.addDocument(document, INDEX_NAME, INDEX_TYPE, 123);
    fixture.commitTransaction();

    verify(httpClient).execute(isA(HttpUriRequest.class));
    verify(httpClient).execute(argument.capture());

    assertEquals("http://host1/_bulk", argument.getValue().getURI().toString());
    assertEquals("{\"index\":{\"_type\":\"bar_type\",\"_index\":\"foo_index\",\"_ttl\":\"123\"}}\n"
            + MESSAGE_CONTENT + "\n", EntityUtils.toString(argument.getValue().getEntity()));
  }

  @Test(expected = MorphlineRuntimeException.class)
  public void shouldThrowEventDeliveryException() throws Exception {
    ArgumentCaptor<HttpPost> argument = ArgumentCaptor.forClass(HttpPost.class);

    when(httpStatus.getStatusCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    when(httpResponse.getStatusLine()).thenReturn(httpStatus);
    when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(httpResponse);

    fixture.addDocument(document, INDEX_NAME, INDEX_TYPE, 123);
    fixture.commitTransaction();
  }

  @Test()
  public void shouldRetryBulkOperation() throws Exception {
    ArgumentCaptor<HttpPost> argument = ArgumentCaptor.forClass(HttpPost.class);

    when(httpStatus.getStatusCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR, HttpStatus.SC_OK);
    when(httpResponse.getStatusLine()).thenReturn(httpStatus);
    when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(httpResponse);

    fixture.addDocument(document, INDEX_NAME, INDEX_TYPE, 123);
    fixture.commitTransaction();

    verify(httpClient, times(2)).execute(isA(HttpUriRequest.class));
    verify(httpClient, times(2)).execute(argument.capture());

    List<HttpPost> allValues = argument.getAllValues();
    assertEquals("http://host1/_bulk", allValues.get(0).getURI().toString());
    assertEquals("http://host2/_bulk", allValues.get(1).getURI().toString());
  }
}
