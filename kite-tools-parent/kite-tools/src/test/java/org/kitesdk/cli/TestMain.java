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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class TestMain {

  private static class SomeException extends RuntimeException {
    private SomeException(String message) {
      super(message);
    }
  }

  @Parameters(commandDescription = "Test description")
  static class TestCommand implements Command {
    @Parameter(description = "<test dataset names>")
    List<String> datasets;

    @Parameter(names = "--throw-arg",
        description = "Causes an IllegalArugmentException", hidden = true)
    boolean throwArg = false;

    @Parameter(names = "--throw-state",
        description = "Causes an IllegalStateException", hidden = true)
    boolean throwState = false;

    @Parameter(names = "--throw-unknown",
        description = "Causes an unknown exception", hidden = true)
    boolean throwUnknown = false;

    @Override
    public int run() throws IOException {
      Preconditions.checkArgument(!throwArg, "--throw-arg was set");
      Preconditions.checkState(!throwState, "--throw-state was set");
      if (throwUnknown) {
        throw new SomeException("--throw-unknown was set");
      }
      return 0;
    }

    @Override
    public List<String> getExamples() {
      return Lists.newArrayList("# this is a comment", "test dataset-name");
    }
  }

  private Logger console;
  private Main main;

  @Before
  public void initializeMain() {
    this.console = mock(Logger.class);
    this.main = new Main(console);
    Command test = new TestCommand();
    main.jc.addCommand("test", test);
  }

  @After
  public void tearDown() {
    // TODO: check line lengths < 80
  }

  @Test
  public void testNoArgs() throws Exception {
    int rc = run();
    verify(console).info(
        contains("Usage: {} [options] [command] [command options]"),
        eq(Main.DEFAULT_PROGRAM_NAME));
    assertEquals(1, rc);
  }

  @Test
  public void testUnrecognizedCommand() throws Exception {
    int rc = run("unrecognizedcommand");
    verify(console).error(startsWith("Expected a command, got unrecognizedcommand"));
    assertEquals(1, rc);
  }

  @Test
  public void testHelp() throws Exception {
    int rc = run("help");
    verify(console).info(
        contains("Usage: {} [options] [command] [command options]"),
        eq(Main.DEFAULT_PROGRAM_NAME));
    verify(console).info(contains("Options"));
    verify(console, times(2)).info(anyString(), any(Object[].class)); // -v, --version
    verify(console).info(contains("Commands"));
    verify(console).info(anyString(), eq("create"), anyString());
    verify(console).info(anyString(), eq("delete"), anyString());
    verify(console).info(anyString(), eq("help"), anyString());
    verify(console).info(anyString(), eq("schema"), anyString());
    verify(console).info(anyString(), eq("csv-schema"), anyString());
    verify(console).info(anyString(), eq("test"), eq("Test description"));
    verify(console).info(contains("Examples"));
    verify(console).info(contains("{} help create"), eq(Main.DEFAULT_PROGRAM_NAME));
    verify(console).info(
        contains("See '{} help <command>' for more information"),
        eq(Main.DEFAULT_PROGRAM_NAME));
    assertEquals(0, rc);
  }

  @Test
  public void testHelpCommand() throws Exception {
    int rc = run("help", "test");
    verify(console).info(
        "\nUsage: {} [general options] {} {} [command options]",
        new Object[]{ Main.DEFAULT_PROGRAM_NAME, "test", "<test dataset names>" });
    verify(console).info(contains("Description"));
    verify(console).info(anyString(), contains("Test description"));
    verify(console).info(contains("Command options"));
    verify(console).info(contains("Examples"));
    verify(console).info(anyString(), contains("# this is a comment"));
    verify(console).info(anyString(),
        eq(new Object[] {Main.DEFAULT_PROGRAM_NAME, "test", "test dataset-name"}));
    assertEquals(0, rc);
  }

  @Test
  public void testCommandHelp() throws Exception {
    int rc = run("test", "--help");
    verify(console).info(
        "\nUsage: {} [general options] {} {} [command options]",
        new Object[]{ Main.DEFAULT_PROGRAM_NAME, "test", "<test dataset names>" });
    verify(console).info(contains("Description"));
    verify(console).info(anyString(), contains("Test description"));
    verify(console).info(contains("Command options"));
    verify(console).info(contains("Examples"));
    verify(console).info(anyString(), contains("this is a comment"));
    verify(console).info(anyString(),
        eq(new Object[] {Main.DEFAULT_PROGRAM_NAME, "test", "test dataset-name"}));
    assertEquals(0, rc);
  }

  @Test
  public void testCommandHelpWithDollarZero() throws Exception {
    int rc = run("--dollar-zero", "datasets", "test", "--help");
    verify(console).info(
        "\nUsage: {} [general options] {} {} [command options]",
        new Object[]{ "datasets", "test", "<test dataset names>" });
    verify(console).info(contains("Description"));
    verify(console).info(anyString(), contains("Test description"));
    verify(console).info(contains("Command options"));
    verify(console).info(contains("Examples"));
    verify(console).info(anyString(), contains("this is a comment"));
    verify(console).info(anyString(),
        eq(new Object[] {"datasets", "test", "test dataset-name"}));
    assertEquals(0, rc);
  }

  @Test
  public void testVersion() throws Exception {
    int rc = run("--version");
    verify(console).info(contains("Kite version"), anyString());
    assertEquals(0, rc);
  }

  @Test
  public void testUnknownCommand() throws Exception {
    int rc = run("help", "unknown");
    verify(console).error(
        contains("Unknown command"),
        eq("unknown"));
    assertEquals(1, rc);
  }

  @Test
  public void testIllegalArguments() throws Exception {
    int rc = run("test", "--throw-arg");
    verify(console).error(
        startsWith("Argument error"),
        eq("--throw-arg was set"));
    assertEquals(1, rc);
  }

  @Test
  public void testIllegalState() throws Exception {
    int rc = run("test", "--throw-state");
    verify(console).error(
        startsWith("State error"),
        eq("--throw-state was set"));
    assertEquals(1, rc);
  }

  @Test
  public void testUnexpectedException() throws Exception {
    int rc = run("test", "--throw-unknown");
    verify(console).error(
        startsWith("Unknown error"),
        eq("--throw-unknown was set"));
    assertEquals(1, rc);
  }

  @Test
  public void testUnexpectedExceptionWithDebug() throws Exception {
    int rc = run("--debug", "test", "--throw-unknown");
    verify(console).error(
        startsWith("Unknown error"),
        isA(SomeException.class));
    assertEquals(1, rc);
  }

  private int run(String... args) throws Exception {
    return main.run(args);
  }
}
