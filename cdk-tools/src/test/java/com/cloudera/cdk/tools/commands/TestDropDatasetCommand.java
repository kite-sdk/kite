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
package com.cloudera.cdk.tools.commands;

import com.cloudera.cdk.data.filesystem.FileSystemDatasetRepository;
import com.cloudera.cdk.data.hcatalog.HCatalogDatasetRepository;
import com.google.common.collect.Lists;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;

public class TestDropDatasetCommand {

  private FileSystemDatasetRepository fsRepo;
  private HCatalogDatasetRepository hcatRepo;

  private DropDatasetCommand command;

  @Before
  public void setUp() throws Exception {
    FileSystemDatasetRepository.Builder fsRepoBuilder = mock
        (FileSystemDatasetRepository.Builder.class);
    fsRepo = mock(FileSystemDatasetRepository.class);
    when(fsRepoBuilder.rootDirectory((URI) anyObject())).thenReturn(fsRepoBuilder);
    when(fsRepoBuilder.configuration((Configuration) anyObject())).thenReturn(fsRepoBuilder);
    when(fsRepoBuilder.get()).thenReturn(fsRepo);

    HCatalogDatasetRepository.Builder hcatRepoBuilder = mock(HCatalogDatasetRepository
        .Builder.class);
    hcatRepo = mock(HCatalogDatasetRepository.class);
    when(hcatRepoBuilder.rootDirectory((URI) anyObject())).thenReturn(hcatRepoBuilder);
    when(hcatRepoBuilder.configuration((Configuration) anyObject())).thenReturn(hcatRepoBuilder);
    when(hcatRepoBuilder.get()).thenReturn(hcatRepo);

    command = new DropDatasetCommand();
    command.fsRepoBuilder = fsRepoBuilder;
    command.hcatRepoBuilder = hcatRepoBuilder;
    command.setConf(new Configuration());
  }

  @Test
  public void testSuccess() throws Exception {
    command.datasetNames = Lists.newArrayList("users", "moreusers");
    command.run();

    verify(hcatRepo).drop("users");
    verify(hcatRepo).drop("moreusers");
  }

}
