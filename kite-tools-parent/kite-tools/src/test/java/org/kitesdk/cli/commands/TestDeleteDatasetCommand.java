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
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.MockRepositories;
import org.kitesdk.data.spi.DatasetRepository;
import org.slf4j.Logger;

import static org.mockito.Mockito.contains;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TestDeleteDatasetCommand {

  private DatasetRepository repo;
  private DeleteDatasetCommand command;
  private Logger console;

  @Before
  public void setUp() {
    this.repo = MockRepositories.newMockRepository();
    this.console = mock(Logger.class);
    this.command = new DeleteDatasetCommand(console);
    this.command.repoURI = repo.getUri().toString();
    verify(repo).getUri(); // verify the above call
  }

  @Test
  public void testBasicUse() throws Exception {
    command.datasets = Lists.newArrayList("users");
    command.run();
    verify(repo).delete("default", "users");
    verify(console).debug(contains("Deleted"), eq("users"));
  }

  @Test
  public void testMultipleDatasets() throws Exception {
    command.datasets = Lists.newArrayList("users", "moreusers");
    command.run();
    verify(repo).delete("default", "users");
    verify(console).debug(contains("Deleted"), eq("users"));
    verify(repo).delete("default", "moreusers");
    verify(console).debug(contains("Deleted"), eq("moreusers"));
  }

}
