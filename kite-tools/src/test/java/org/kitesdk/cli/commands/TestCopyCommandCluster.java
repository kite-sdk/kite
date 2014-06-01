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

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Sets;
import com.google.common.io.Files;
import java.io.BufferedWriter;
import java.io.File;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.cli.TestUtil;
import org.kitesdk.data.DatasetRepositories;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.spi.filesystem.DatasetTestUtilities;
import org.slf4j.Logger;

import static org.mockito.Mockito.mock;

public class TestCopyCommandCluster extends MiniDFSTest {

  private static final String source = "users_source";
  private static final String dest = "users_dest";
  private static final String avsc = "target/user.avsc";
  private static String repoUri;

  @BeforeClass
  public static void createSourceDataset() throws Exception {
    repoUri = "hdfs://" + getDFS().getUri().getAuthority() + "/tmp/data";
    TestUtil.run("delete", source, "-r", repoUri, "-d", "target/data");

    String csv = "target/users.csv";
    BufferedWriter writer = Files.newWriter(
        new File(csv), CSVSchemaCommand.SCHEMA_CHARSET);
    writer.append("id,username,email\n");
    writer.append("1,test,test@example.com\n");
    writer.append("2,user,user@example.com\n");
    writer.close();

    TestUtil.run("-v", "csv-schema", csv, "-o", avsc, "--class", "User");
    TestUtil.run("create", source, "-s", avsc,
        "-r", repoUri, "-d", "target/data");
    TestUtil.run("csv-import", csv, source, "-r", repoUri, "-d", "target/data");
  }

  @AfterClass
  public static void deleteSourceDataset() throws Exception {
    TestUtil.run("delete", source, "-r", repoUri, "-d", "target/data");
  }

  private Logger console;
  private CopyCommand command;

  @Before
  public void createDestination() throws Exception {
    TestUtil.run("delete", dest, "-r", repoUri, "-d", "target/data");
    TestUtil.run("create", dest, "-s", avsc, "-r", repoUri, "-d", "target/data");
    this.console = mock(Logger.class);
    this.command = new CopyCommand(console);
    command.setConf(new Configuration());
  }

  @After
  public void deleteDestination() throws Exception {
    TestUtil.run("delete", dest, "-r", repoUri, "-d", "target/data");
  }

  @Test
  public void testBasicCopy() throws Exception {
    command.repoURI = repoUri;
    command.datasets = Lists.newArrayList(source, dest);

    int rc = command.run();
    Assert.assertEquals("Should return success", 0, rc);

    DatasetRepository repo = DatasetRepositories.open("repo:" + repoUri);
    int size = DatasetTestUtilities.datasetSize(repo.load(source));
    Assert.assertEquals("Should contain copied records", 2, size);
  }
}
