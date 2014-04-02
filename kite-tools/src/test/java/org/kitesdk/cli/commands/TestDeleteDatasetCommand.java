/**
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
package org.kitesdk.cli.commands;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.spi.OptionBuilder;
import org.kitesdk.data.spi.URIPattern;

import static org.mockito.Mockito.*;

public class TestDeleteDatasetCommand {

  private static AtomicInteger ids = new AtomicInteger(0);
  private static Map<String, DatasetRepository> repos = Maps.newHashMap();
  private String id;
  private DeleteDatasetCommand command;

  @BeforeClass
  public static void addMockRepoBuilder() throws Exception {
    org.kitesdk.data.impl.Accessor.getDefault().registerDatasetRepository(
        new URIPattern("mock::id"), new OptionBuilder<DatasetRepository>() {
          @Override
          public DatasetRepository getFromOptions(Map<String, String> options) {
            DatasetRepository repo = mock(DatasetRepository.class);
            repos.put(options.get("id"), repo);
            return repo;
          }
        });
  }

  @Before
  public void setUp() {
    this.id = Integer.toString(ids.addAndGet(1));
    this.command = new DeleteDatasetCommand();
    this.command.repoURI = "repo:mock:" + id;
  }

  public DatasetRepository getMockRepo() {
    return repos.get(id);
  }

  @Test
  public void testBasicUse() throws Exception {
    command.datasetNames = Lists.newArrayList("users");
    command.run();
    verify(getMockRepo()).delete("users");
  }

  @Test
  public void testMultipleDatasets() throws Exception {
    command.datasetNames = Lists.newArrayList("users", "moreusers");
    command.run();
    verify(getMockRepo()).delete("users");
    verify(getMockRepo()).delete("moreusers");
  }

}
