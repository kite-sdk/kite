/*
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
package org.kitesdk.morphline.stdlib;

import java.util.Arrays;
import java.util.List;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.base.AbstractCommand;

import com.typesafe.config.Config;

/**
 * A morphline has a name and contains a chain of zero or more commands, through which the morphline
 * pipes each input record. A command transforms the record into zero or more records.
 */
final class Pipe extends AbstractCommand {
  
  private final String id;
  private final Command realChild;

  public Pipe(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
    super(builder, config, parent, child, context);
    this.id = getConfigs().getString(config, "id");

    List<String> importCommandSpecs = getConfigs().getStringList(config, "importCommands", 
        Arrays.asList("com.**", "org.**", "net.**"));    
    context.importCommandBuilders(importCommandSpecs);

    getConfigs().getConfigList(config, "commands", null);
    List<Command> childCommands = buildCommandChain(config, "commands", child, false);
    if (childCommands.size() > 0) {
      this.realChild = childCommands.get(0);
    } else {
      this.realChild = child;
    }
    validateArguments();
  }
  
  @Override
  protected Command getChild() {
    return realChild;
  }
  
}
