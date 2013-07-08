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
package com.cloudera.cdk.tools;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.MissingCommandException;
import com.beust.jcommander.ParameterException;
import com.cloudera.cdk.tools.commands.CreateDatasetCommand;
import com.cloudera.cdk.tools.commands.DropDatasetCommand;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {

  private JCommander jc;
  private Help help = new Help();

  private static Set<String> HELP_ARGS = ImmutableSet.of("-h", "-help", "--help", "help");

  private static final Map<String, Command> COMMANDS = ImmutableSortedMap.<String, Command>naturalOrder()
      .put("create-dataset", new CreateDatasetCommand())
      .put("drop-dataset", new DropDatasetCommand())
      .build();

  Main() {
    jc = new JCommander();
    jc.setProgramName("cdk");
    jc.addCommand("help", help, "-h", "-help", "--help");
    for (Map.Entry<String, Command> e : COMMANDS.entrySet()) {
      jc.addCommand(e.getKey(), e.getValue());
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length == 0) {
      usage();
      return 1;
    }

    try {
      jc.parse(args);
    } catch (MissingCommandException e) {
      System.err.println(e.getMessage());
      return 1;
    } catch (ParameterException e) {
      String cmd = jc.getParsedCommand();
      if (args.length == 1) { // i.e., just the command (missing required arguments)
        jc.usage(cmd);
        return 1;
      } else { // check for variants like 'cmd --help' etc.
        for (String arg : args) {
          if (HELP_ARGS.contains(arg)) {
            jc.usage(cmd);
            return 0;
          }
        }
      }
      System.err.println(e.getMessage());
      return 1;
    }

    if ("help".equals(jc.getParsedCommand())) {
      return usage();
    }

    Command cmd = COMMANDS.get(jc.getParsedCommand());
    if (cmd == null) {
      return usage();
    }
    try {
      if (cmd instanceof Configurable) {
        ((Configurable) cmd).setConf(getConf());
      }
      return cmd.run();
    } catch (IllegalArgumentException e) {
      System.err.println("Argument Error: " + e.getMessage());
      return 1;
    } catch (IllegalStateException e) {
      System.err.println("State Error: " + e.getMessage());
      return 1;
    }
  }

  private int usage() {
    return help.usage(jc, COMMANDS);
  }

  public static void main(String[] args) throws Exception {
    int rc = ToolRunner.run(new Configuration(), new Main(), args);
    System.exit(rc);
  }
}
