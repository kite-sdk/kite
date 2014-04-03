/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.cli.commands;

import java.io.IOException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.TestHelpers;
import org.mockito.Mockito;
import org.slf4j.Logger;

import static org.mockito.Mockito.*;

/**
 * This tests that the base command correctly builds a repository URI from the
 * given options.
 */
public class TestBaseCommand {

  public static class TestCommand extends BaseDatasetCommand {
    public TestCommand(Logger console) {
      super(console);
    }

    @Override
    public int run() throws IOException {
      return 0;
    }
  }

  private Logger console = null;
  private BaseDatasetCommand command = null;

  @Before
  public void createCommand() {
    this.console = mock(Logger.class);
    this.command = new TestCommand(console);
  }

  @Test
  public void testDefaults() {
    Assert.assertEquals("repo:hive", command.buildRepoURI());
    verify(console).trace(contains("repo:hive"));
  }

  @Test
  public void testManagedHiveRepo() {
    command.hcatalog = true;
    command.directory = null;
    command.local = false;
    Assert.assertEquals("repo:hive", command.buildRepoURI());
    verify(console).trace(contains("repo:hive"));
  }

  @Test
  public void testManagedHiveRepoLocal() {
    command.hcatalog = true;
    command.directory = null;
    command.local = true;
    Assert.assertEquals("repo:hive", command.buildRepoURI());
    verify(console).trace(contains("repo:hive"));
  }

  @Test
  public void testExternalHiveRepo() {
    command.hcatalog = true;
    command.directory = "/tmp/data";
    Assert.assertEquals("repo:hive:/tmp/data", command.buildRepoURI());
    verify(console).trace(contains("repo:hive:/tmp/data"));
  }

  @Test
  public void testRelativeExternalHiveRepo() {
    command.hcatalog = true;
    command.directory = "data";
    Assert.assertEquals("repo:hive:data", command.buildRepoURI());
    verify(console).trace(contains("repo:hive:data"));
  }

  @Test
  public void testHDFSRepo() {
    command.hcatalog = false;
    command.directory = "/tmp/data";
    Assert.assertEquals("repo:hdfs:/tmp/data", command.buildRepoURI());
    verify(console).trace(contains("repo:hdfs:/tmp/data"));
  }

  @Test
  public void testLocalRepo() {
    command.hcatalog = false;
    command.local = true;
    command.directory = "/tmp/data";
    Assert.assertEquals("repo:file:/tmp/data", command.buildRepoURI());
    verify(console).trace(contains("repo:file:/tmp/data"));
  }

  @Test
  public void testHDFSRepoRejectsNullPath() {
    command.hcatalog = false;
    command.local = false;
    command.directory = null;
    TestHelpers.assertThrows(
        "Should reject null directory for HDFS, non-Hive",
        IllegalArgumentException.class,
        new Runnable() {
          @Override
          public void run() {
            command.buildRepoURI();
          }
        }
    );
    verifyZeroInteractions(console);
  }

  @Test
  public void testLocalRepoRejectsNullPath() {
    command.hcatalog = false;
    command.local = true;
    command.directory = null;
    TestHelpers.assertThrows(
        "Should reject null directory for local, non-Hive",
        IllegalArgumentException.class,
        new Runnable() {
          @Override
          public void run() {
            command.buildRepoURI();
          }
        });
    verifyZeroInteractions(console);
  }

}
