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

import com.codahale.metrics.MetricRegistry;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.impl.ConfigImpl;
import java.util.Arrays;
import java.util.HashSet;
import static org.junit.Assert.assertNotNull;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class LoadElasticsearchBuilderTest {

  @Mock
  private Config config;

  @Mock
  private Config esConfig;

  @Mock
  private Command parent;

  @Mock
  private Command child;

  @Mock
  private MorphlineContext context;
  
  private LoadElasticsearchBuilder fixture;

  @Before
  public void setUp() {
    initMocks(this);
    fixture = new LoadElasticsearchBuilder();
    
    when(esConfig.getStringList(DocumentLoaderFactory.HOSTS_FIELD)).thenReturn(Arrays.asList("test123"));
    when(esConfig.getString(DocumentLoaderFactory.CLUSTER_NAME_FIELD)).thenReturn("test");
    when(config.getConfig(LoadElasticsearchBuilder.ELASTICSEARCH_CONFIGURATION)).thenReturn(esConfig);
    when(config.getString(LoadElasticsearchBuilder.DOCUMENT_LOADER_TYPE))
            .thenReturn(DocumentLoaderFactory.TRANSPORT_DOCUMENT_LOADER);
    ConfigObject obj = mock(ConfigObject.class);
    when(obj.keySet()).thenReturn(new HashSet<String>());
    when(config.root()).thenReturn(obj);
  }

  @Test
  public void testGetNames() {
    assertNotNull(fixture.getNames());
  }

  @Test
  public void testBuild() {
    when(context.getMetricRegistry()).thenReturn(new MetricRegistry());
    fixture.build(config, parent, child, context);
  }
}
