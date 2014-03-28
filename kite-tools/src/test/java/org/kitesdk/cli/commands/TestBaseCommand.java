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
import org.junit.Test;

/**
 * This tests that the base command correctly builds a repository URI from the
 * given options.
 */
public class TestBaseCommand {

  public static class TestCommand extends BaseDatasetCommand {
    @Override
    public int run() throws IOException {
      return 0;
    }
  }

  @Test
  public void testDefaults() {
    BaseDatasetCommand command = new TestCommand();
    Assert.assertEquals("repo:hive", command.buildRepoURI());
  }

  @Test
  public void testManagedHiveRepo() {
    BaseDatasetCommand command = new TestCommand();
    command.hcatalog = true;
    command.directory = null;
    command.local = true;
    Assert.assertEquals("repo:hive", command.buildRepoURI());
    command.local = false;
    Assert.assertEquals("repo:hive", command.buildRepoURI());
  }

  @Test
  public void testExternalHiveRepo() {
    BaseDatasetCommand command = new TestCommand();
    command.hcatalog = true;
    command.directory = "/tmp/data";
    Assert.assertEquals("repo:hive:/tmp/data", command.buildRepoURI());
  }

  @Test
  public void testRelativeExternalHiveRepo() {
    BaseDatasetCommand command = new TestCommand();
    command.hcatalog = true;
    command.directory = "data";
    Assert.assertEquals("repo:hive:data", command.buildRepoURI());
  }

  @Test
  public void testHDFSRepo() {
    BaseDatasetCommand command = new TestCommand();
    command.hcatalog = false;
    command.directory = "/tmp/data";
    Assert.assertEquals("repo:hdfs:/tmp/data", command.buildRepoURI());
  }

  @Test
  public void testLocalRepo() {
    BaseDatasetCommand command = new TestCommand();
    command.hcatalog = false;
    command.local = true;
    command.directory = "/tmp/data";
    Assert.assertEquals("repo:file:/tmp/data", command.buildRepoURI());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testHDFSRepoRejectsNullPath() {
    final BaseDatasetCommand command = new TestCommand();
    command.hcatalog = false;
    command.local = true;
    command.directory = null;
    command.buildRepoURI();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testLocalRepoRejectsNullPath() {
    final BaseDatasetCommand command = new TestCommand();
    command.hcatalog = false;
    command.local = true;
    command.directory = null;
    command.buildRepoURI();
  }

}
