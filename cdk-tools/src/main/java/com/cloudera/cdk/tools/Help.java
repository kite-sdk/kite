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
package com.cloudera.cdk.tools;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;

@Parameters(commandDescription = "Retrieves details on the functions of other commands")
public class Help {
  @Parameter(description = "Commands")
  List<String> helpCommands = Lists.newArrayList();

  public int usage(JCommander jc, Map<String, Command> cmds) {
    if (helpCommands.isEmpty()) {
      System.out.println("Usage: cdk [options] [command] [command options]\n");
      System.out.println("The following commands are available:");
      for (Map.Entry<String, Command> e : cmds.entrySet()) {
        Parameters parameters = e.getValue().getClass().getAnnotation(Parameters.class);
        if (parameters != null) {
          System.out.println(String.format("   %s\t\t\t%s", e.getKey(),
              parameters.commandDescription()));
        }
      }
      System.out.println("\nSee 'cdk help <command>' for more information on a specific" +
          " command.");
    } else {
      for (String cmd : helpCommands) {
        jc.usage(cmd);
      }
    }
    return 0;
  }
}
