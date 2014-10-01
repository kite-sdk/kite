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
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import java.io.BufferedWriter;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.cli.TestUtil;
import org.kitesdk.cli.example.User;
import org.kitesdk.data.MiniDFSTest;
import org.slf4j.Logger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TestSchemaCommandCluster extends MiniDFSTest {
  private static String repoURI = null;
  private Logger console = null;

  @BeforeClass
  public static void createCSVSample() throws Exception {
    repoURI = "repo:" + getDFS().getUri().toString() + "/tmp/data";
  }

  @Before
  public void setup() throws Exception {
    this.console = mock(Logger.class);
  }

  @Test
  public void testObjSchemaToFile() throws Exception {
    Schema schema = ReflectData.get().getSchema(User.class);

    ObjectSchemaCommand command = new ObjectSchemaCommand(console);
    command.setConf(getConfiguration());
    command.classNames = Lists.newArrayList("org.kitesdk.cli.example.User");
    command.outputPath = "target/obj.avsc";
    int rc = command.run();
    Assert.assertEquals("Should return success code", 0, rc);
    String fileContent = Files.toString(
        new File("target/obj.avsc"), BaseCommand.UTF8);
    Assert.assertTrue("File should contain pretty printed schema",
        TestUtil.matchesSchema(schema).matches(fileContent));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testObjSchemaToHDFSFile() throws Exception {
    Schema schema = ReflectData.get().getSchema(User.class);

    String hdfsSchemaPath = "hdfs:/tmp/schemas/obj.avsc";
    ObjectSchemaCommand command = new ObjectSchemaCommand(console);
    command.setConf(getConfiguration());
    command.classNames = Lists.newArrayList("org.kitesdk.cli.example.User");
    command.outputPath = hdfsSchemaPath;
    int rc = command.run();
    Assert.assertEquals("Should return success code", 0, rc);
    String fileContent = CharStreams.toString(
        new InputStreamReader(getDFS().open(new Path(hdfsSchemaPath)), "utf8"));
    Assert.assertTrue("File should contain pretty printed schema",
        TestUtil.matchesSchema(schema).matches(fileContent));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testCSVSchemaToFile() throws Exception {
    String csvSample = "target/users.csv";
    BufferedWriter writer = Files.newWriter(
        new File(csvSample), CSVSchemaCommand.SCHEMA_CHARSET);
    writer.append("id, username, email\n");
    writer.append("1, test, test@example.com\n");
    writer.close();

    Schema schema = SchemaBuilder.record("User").fields()
        .optionalLong("id")
        .optionalString("username")
        .optionalString("email")
        .endRecord();

    CSVSchemaCommand command = new CSVSchemaCommand(console);
    command.setConf(getConfiguration());
    command.samplePaths = Lists.newArrayList(csvSample);
    command.outputPath = "target/csv.avsc";
    command.recordName = "User";

    int rc = command.run();
    Assert.assertEquals("Should return success code", 0, rc);
    String fileContent = Files.toString(
        new File("target/csv.avsc"), BaseCommand.UTF8);
    Assert.assertTrue("File should contain pretty printed schema",
        TestUtil.matchesSchema(schema).matches(fileContent));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testCSVSchemaToHDFSFile() throws Exception {
    String csvSample = "target/users.csv";
    BufferedWriter writer = Files.newWriter(
        new File(csvSample), CSVSchemaCommand.SCHEMA_CHARSET);
    writer.append("id, username, email\n");
    writer.append("1, test, test@example.com\n");
    writer.close();

    Schema schema = SchemaBuilder.record("User").fields()
        .optionalLong("id")
        .optionalString("username")
        .optionalString("email")
        .endRecord();

    String hdfsSchemaPath = "hdfs:/tmp/schemas/csv.avsc";
    CSVSchemaCommand command = new CSVSchemaCommand(console);
    command.setConf(getConfiguration());
    command.samplePaths = Lists.newArrayList(csvSample);
    command.outputPath = hdfsSchemaPath;
    command.recordName = "User";
    int rc = command.run();
    Assert.assertEquals("Should return success code", 0, rc);
    String fileContent = CharStreams.toString(
        new InputStreamReader(getDFS().open(new Path(hdfsSchemaPath)), "utf8"));
    Assert.assertTrue("File should contain pretty printed schema",
        TestUtil.matchesSchema(schema).matches(fileContent));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testHDFSCSVSchemaToFile() throws Exception {
    String csvSample = "hdfs:/tmp/sample/users.csv";
    FSDataOutputStream out = getDFS()
        .create(new Path(csvSample), true /* overwrite */);
    OutputStreamWriter writer = new OutputStreamWriter(out, "utf8");
    writer.append("id, username, email\n");
    writer.append("1, test, test@example.com\n");
    writer.close();

    Schema schema = SchemaBuilder.record("User").fields()
        .optionalLong("id")
        .optionalString("username")
        .optionalString("email")
        .endRecord();

    CSVSchemaCommand command = new CSVSchemaCommand(console);
    command.setConf(getConfiguration());
    command.samplePaths = Lists.newArrayList(csvSample);
    command.outputPath = "target/csv2.avsc";
    command.recordName = "User";

    int rc = command.run();
    Assert.assertEquals("Should return success code", 0, rc);
    String fileContent = Files.toString(
        new File("target/csv2.avsc"), BaseCommand.UTF8);
    Assert.assertTrue("File should contain pretty printed schema",
        TestUtil.matchesSchema(schema).matches(fileContent));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testHDFSCSVSchemaToHDFSFile() throws Exception {
    String csvSample = "hdfs:/tmp/sample/users.csv";
    FSDataOutputStream out = getDFS()
        .create(new Path(csvSample), true /* overwrite */);
    OutputStreamWriter writer = new OutputStreamWriter(out, "utf8");
    writer.append("id, username, email\n");
    writer.append("1, test, test@example.com\n");
    writer.close();

    Schema schema = SchemaBuilder.record("User").fields()
        .optionalLong("id")
        .optionalString("username")
        .optionalString("email")
        .endRecord();

    String hdfsSchemaPath = "hdfs:/tmp/schemas/csv2.avsc";
    CSVSchemaCommand command = new CSVSchemaCommand(console);
    command.setConf(getConfiguration());
    command.samplePaths = Lists.newArrayList(csvSample);
    command.outputPath = hdfsSchemaPath;
    command.recordName = "User";
    int rc = command.run();
    Assert.assertEquals("Should return success code", 0, rc);
    String fileContent = CharStreams.toString(
        new InputStreamReader(getDFS().open(new Path(hdfsSchemaPath)), "utf8"));
    Assert.assertTrue("File should contain pretty printed schema",
        TestUtil.matchesSchema(schema).matches(fileContent));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testDatasetSchemaToFile() throws Exception {
    Schema schema = new Schema.Parser().parse(
        Resources.getResource("schema/user.avsc").openStream());

    TestUtil.run("create", "users", "--schema", "resource:schema/user.avsc",
        "-r", repoURI);

    SchemaCommand command = new SchemaCommand(console);
    command.setConf(getConfiguration());
    command.datasets = Lists.newArrayList("users");
    command.outputPath = "target/user.avsc";
    command.repoURI = repoURI;
    int rc = command.run();
    Assert.assertEquals("Should return success code", 0, rc);
    String fileContent = Files.toString(
        new File("target/user.avsc"), BaseCommand.UTF8);
    Assert.assertTrue("File should contain pretty printed schema",
        TestUtil.matchesSchema(schema).matches(fileContent));
    verifyNoMoreInteractions(console);

    TestUtil.run("delete", "users", "-r", repoURI);
  }

  @Test
  public void testDatasetSchemaToHDFSFile() throws Exception {
    Schema schema = new Schema.Parser().parse(
        Resources.getResource("schema/user.avsc").openStream());

    TestUtil.run("create", "users", "--schema", "resource:schema/user.avsc",
        "-r", repoURI);

    String hdfsSchemaPath = "hdfs:/tmp/schemas/user.avsc";
    SchemaCommand command = new SchemaCommand(console);
    command.setConf(getConfiguration());
    command.datasets = Lists.newArrayList("users");
    command.outputPath = hdfsSchemaPath;
    command.repoURI = repoURI;
    int rc = command.run();
    Assert.assertEquals("Should return success code", 0, rc);
    String fileContent = CharStreams.toString(
        new InputStreamReader(getDFS().open(new Path(hdfsSchemaPath)), "utf8"));
    Assert.assertTrue("File should contain pretty printed schema",
        TestUtil.matchesSchema(schema).matches(fileContent));
    verifyNoMoreInteractions(console);

    TestUtil.run("delete", "users", "-r", repoURI);
  }
}
