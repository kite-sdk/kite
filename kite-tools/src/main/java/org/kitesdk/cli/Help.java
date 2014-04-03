/**
 * Copyright (c) 2013, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package org.kitesdk.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterDescription;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;
import java.util.List;
import org.slf4j.Logger;

@Parameters(commandDescription = "Retrieves details on the functions of other commands")
public class Help {
  @Parameter(description = "Commands")
  List<String> helpCommands = Lists.newArrayList();

  private final JCommander jc;
  private final Logger console;

  public Help(JCommander jc, Logger console) {
    this.jc = jc;
    this.console = console;
  }

  public int run() {
    if (helpCommands.isEmpty()) {
      console.info(
          "Usage: {} [options] [command] [command options]",
          Main.PROGRAM_NAME);
      console.info("\n  Options:");
      for (ParameterDescription param : jc.getParameters()) {
        printOption(console, param);
      }
      console.info("\n  Commands:");
      for (String command : jc.getCommands().keySet()) {
        console.info("    {}\n\t{}",
            command, jc.getCommandDescription(command));
      }
      console.info("\n  See '{} help <command>' for more information on a " +
          "specific command.", Main.PROGRAM_NAME);

    } else {
      for (String cmd : helpCommands) {
        JCommander commander = jc.getCommands().get(cmd);
        if (commander == null) {
          console.error("Unknown command: {}", cmd);
          return 1;
        }

        console.info(jc.getCommandDescription(cmd));
        console.info("Usage: {} [general options] {} [command options] {}",
            new Object[] {
                Main.PROGRAM_NAME, cmd,
                commander.getMainParameterDescription()});
        console.info("\n  Command options:");
        for (ParameterDescription param : commander.getParameters()) {
          printOption(console, param);
        }
        // add an extra newline in case there are more commands
        console.info("");
      }

      console.info("  * = required\n");
    }
    return 0;
  }

  private void printOption(Logger console, ParameterDescription param) {
    if (!param.getParameter().hidden()) {
      console.info("  {} {}\n\t{}{}", new Object[]{
          param.getParameter().required() ? "*" : " ",
          param.getNames().trim(),
          param.getDescription(),
          formatDefault(param)});
    }
  }

  private String formatDefault(ParameterDescription param) {
    Object defaultValue = param.getDefault();
    if (defaultValue == null) {
      return "";
    }
    return " (default: " + ((defaultValue instanceof String) ?
        "\"" + defaultValue + "\"" :
        defaultValue.toString()) + ")";
  }
}
