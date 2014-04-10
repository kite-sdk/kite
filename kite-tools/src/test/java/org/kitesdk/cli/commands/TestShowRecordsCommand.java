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

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import java.io.BufferedWriter;
import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.cli.TestUtil;
import org.slf4j.Logger;

import static org.mockito.Mockito.*;

public class TestShowRecordsCommand {

  private Logger console = null;
  private ShowRecordsCommand command;

  @BeforeClass
  public static void buildUserSchema() throws Exception {
    String sample = "target/users.csv";
    BufferedWriter writer = Files.newWriter(
        new File(sample), CSVSchemaCommand.SCHEMA_CHARSET);
    writer.append("id,username,email\n");
    writer.append("1,test,test@example.com\n");
    writer.append("2,user,user@example.com\n");
    writer.close();

    TestUtil.run("-v", "csv-schema", "target/users.csv",
        "-o", "target/user.avsc", "--class", "User");
    TestUtil.run("-v", "create", "users",
        "--local", "-d", "target/data", "-s", "target/user.avsc");
    TestUtil.run("-v", "csv-import", "target/users.csv",
        "--local", "-d", "target/data", "users");
  }

  @AfterClass
  public static void removeData() throws Exception {
    TestUtil.run("delete", "users", "--local", "-d", "target/data");
  }

  @Before
  public void setup() throws Exception {
    this.console = mock(Logger.class);
    this.command = new ShowRecordsCommand(console);
    command.setConf(new Configuration());
    // set the test repository information
    command.local = true;
    command.directory = "target/data";
  }

  @Test
  public void testDefaultArgs() throws Exception {
    command.datasetNames = Lists.newArrayList("users");
    command.run();
    verify(console).trace(contains("repo:file:target/data"));
    verify(console).info(
        "{\"id\": 1, \"username\": \"test\", \"email\": \"test@example.com\"}");
    verify(console).info(
        "{\"id\": 2, \"username\": \"user\", \"email\": \"user@example.com\"}");
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testNumRecords() throws Exception {
    command.datasetNames = Lists.newArrayList("users");
    command.numRecords = 1;
    command.run();
    verify(console).trace(contains("repo:file:target/data"));
    verify(console).info(
        "{\"id\": 1, \"username\": \"test\", \"email\": \"test@example.com\"}");
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testZeroRecords() throws Exception {
    command.datasetNames = Lists.newArrayList("users");
    command.numRecords = 0;
    command.run();
    verify(console).trace(contains("repo:file:target/data"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testNegativeNumRecords() throws Exception {
    command.datasetNames = Lists.newArrayList("users");
    command.numRecords = -1;
    command.run();
    verify(console).trace(contains("repo:file:target/data"));
    verifyNoMoreInteractions(console);
  }

}
