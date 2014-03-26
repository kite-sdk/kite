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

import com.typesafe.config.Config;
import java.util.Arrays;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import static org.mockito.Matchers.any;
import org.mockito.Mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class DocumentLoaderFactoryTest {

  @Mock
  private Config config;

  private DocumentLoaderFactory factory;

  @Before
  public void setUp() {
    factory = new DocumentLoaderFactory();
    initMocks(this);
  }
  
  @Test
  public void testGetTransportClient() throws NoSuchDocumentLoaderException {
    when(config.getStringList(DocumentLoaderFactory.HOSTS_FIELD)).thenReturn(Arrays.asList("test123"));
    when(config.getString(DocumentLoaderFactory.CLUSTER_NAME_FIELD)).thenReturn("cluster");
    DocumentLoader client = factory.getClient(DocumentLoaderFactory.TRANSPORT_DOCUMENT_LOADER, config);
    assertNotNull(client);
    assertTrue(client instanceof TransportDocumentLoader);
  }

  @Test
  public void testGetRestClient() throws NoSuchDocumentLoaderException {
    when(config.getStringList(any(String.class))).thenReturn(Arrays.asList("test123"));
    DocumentLoader client = factory.getClient(DocumentLoaderFactory.REST_DOCUMENT_LOADER, config);
    assertNotNull(client);
    assertTrue(client instanceof RestDocumentLoader);
  }

  @Test(expected = NoSuchDocumentLoaderException.class)
  public void testThrowsNoSuchDocumentLoaderException() throws NoSuchDocumentLoaderException {
    when(config.getStringList(any(String.class))).thenReturn(Arrays.asList(""));
    DocumentLoader client = factory.getClient("test123123", config);
  }

  @Test(expected = MorphlineRuntimeException.class)
  public void testNoParameterHostnamesSet() throws NoSuchDocumentLoaderException {
    when(config.getStringList(any(String.class))).thenReturn(null);
    DocumentLoader client = factory.getClient(DocumentLoaderFactory.TRANSPORT_DOCUMENT_LOADER, config);
  }

  @Test(expected = MorphlineRuntimeException.class)
  public void testNoParameterClusternameSet() throws NoSuchDocumentLoaderException {
    when(config.getStringList(DocumentLoaderFactory.HOSTS_FIELD)).thenReturn(Arrays.asList("test123"));
    when(config.getString(DocumentLoaderFactory.CLUSTER_NAME_FIELD)).thenReturn(null);
    DocumentLoader client = factory.getClient(DocumentLoaderFactory.TRANSPORT_DOCUMENT_LOADER, config);
  }
}
