/**
 * Copyright 2014 Cloudera Inc.
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
package org.kitesdk.minicluster.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.MissingCommandException;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;
import org.kitesdk.minicluster.MiniCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Parameters(commandDescription = "Kite minicluster utility")
public class Main extends Configured implements Tool {

  public static final String PROGRAM_NAME = "minicluster";

  private static Set<String> HELP_ARGS = ImmutableSet.of("-h", "-help",
      "--help", "help");

  private final Logger console;
  private final Help help;
  private final JCommander jc;

  Main(Logger console) {
    this.console = console;
    this.jc = new JCommander(this);
    this.help = new Help(jc, console);

    jc.setProgramName(PROGRAM_NAME);
    jc.addCommand("help", help, "-h", "-help", "--help");
    jc.addCommand("run", new RunCommand(console, getConf()));
  }

  @Override
  public int run(String[] args) throws Exception {
    try {
      jc.parse(args);
    } catch (MissingCommandException e) {
      console.error(e.getMessage());
      return 1;
    } catch (ParameterException e) {
      String cmd = jc.getParsedCommand();
      if (args.length == 1) { // i.e., just the command (missing required
                              // arguments)
        help.helpCommands.add(cmd);
        help.run();
        return 1;
      } else { // check for variants like 'cmd --help' etc.
        for (String arg : args) {
          if (HELP_ARGS.contains(arg)) {
            help.helpCommands.add(cmd);
            help.run();
            return 0;
          }
        }
      }
      console.error(e.getMessage());
      return 1;
    }

    String parsed = jc.getParsedCommand();
    if (parsed == null) {
      help.run();
      return 1;
    } else if ("help".equals(parsed)) {
      return help.run();
    }

    Command command = (Command) jc.getCommands().get(parsed).getObjects()
        .get(0);
    if (command == null) {
      help.run();
      return 1;
    }

    try {
      return command.run();
    } catch (IllegalArgumentException e) {
      console.error("Argument error: {}", e.getMessage());
      return 1;
    } catch (IllegalStateException e) {
      console.error("State error: {}", e.getMessage());
      return 1;
    } catch (Exception e) {
      console.error("Unknown error: {}", e.getMessage());
      console.error("ERROR", e);
      return 1;
    }
  }

  public static void main(String[] args) throws Exception {
    // reconfigure logging with the kite minicluster configuration
    PropertyConfigurator.configure(MiniCluster.class
        .getResource("/kite-minicluster-logging.properties"));
    Logger console = LoggerFactory.getLogger(MiniCluster.class);
    int rc = ToolRunner.run(new Configuration(), new Main(console), args);
    System.exit(rc);
  }
}
