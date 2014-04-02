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
package org.kitesdk.cli;

import com.google.common.base.Splitter;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.cli.Main;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestMain {

  private PrintStream sysout, syserr;
  private ByteArrayOutputStream out, err;

  @Before
  public void setUp() {
    sysout = System.out;
    out = new ByteArrayOutputStream();
    System.setOut(new PrintStream(out, true));

    syserr = System.err;
    err = new ByteArrayOutputStream();
    System.setErr(new PrintStream(err, true));
  }

  @After
  public void tearDown() {
    System.setOut(sysout);
    System.setErr(syserr);

    String outString = out.toString();
    for (String line : Splitter.on("\n").split(outString)) {
      assertTrue("Line should be no more than 80 chars: " + line,
          line.replace("\t", "        ").length() <= 80);
    }
  }

  @Test
  public void testNoArgs() throws Exception {
    int rc = run();
    String outString = out.toString();
    assertTrue(outString, outString.startsWith(
        "Usage: " + Main.PROGRAM_NAME +
        " [options] [command] [command options]"));
    assertEquals(1, rc);
  }

  @Test
  public void testUnrecognizedCommand() throws Exception {
    int rc = run("unrecognizedcommand");
    String errString = err.toString();
    assertTrue(errString, errString.startsWith(
        "Expected a command, got unrecognizedcommand"));
    assertEquals(1, rc);
  }

  @Test
  public void testHelp() throws Exception {
    int rc = run("help");
    String outString = out.toString();
    assertTrue(outString, outString.startsWith(
        "Usage: " + Main.PROGRAM_NAME +
        " [options] [command] [command options]"));
    assertEquals(0, rc);
  }

  @Test
  public void testHelpCommand() throws Exception {
    int rc = run("help", "create");
    String outString = out.toString();
    assertTrue(outString, outString.startsWith(
        "Create an empty dataset\nUsage: create [options] <dataset name>"));
    assertEquals(0, rc);
  }

  @Test
  public void testCommandHelp() throws Exception {
    int rc = run("create", "--help");
    String outString = out.toString();
    assertTrue(outString, outString.startsWith(
        "Create an empty dataset\nUsage: create [options] <dataset name>"));
    assertEquals(0, rc);
  }

  private int run(String... args) throws Exception {
    return new Main().run(args);
  }
}
