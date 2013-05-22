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
package com.cloudera.cdk.morphline.stdlib;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.CommandBuilder;
import com.cloudera.cdk.morphline.api.MorphlineCompilationException;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.AbstractCommand;
import com.typesafe.config.Config;

/**
 * An If-Then-Else command consists of a chain of zero or more conditions commands, as well as a
 * chain of zero or or more commands that are processed if all conditions succeed ("then commands"),
 * as well as a chain of zero or more commands that are processed if one of the conditions fails
 * ("else commands").
 * 
 * If one of the commands in the "then" chain or "else" chain fails then the entire "if" command
 * fails (and the remaining commands in the "then" or "else" branch are skipped).
 * 
 * Example:
 * 
 * <pre>
 *         if { 
 *           conditions : [
 * #            { fail {} }
 *           ]
 *           then : [
 *             { logInfo { format : "processing then..." } }
 *           ]
 *           else : [
 *             { logInfo { format : "processing else..." } }
 *           ]
 *         }
 * </pre>
 */
public final class IfThenElseBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("if");
  }
  
  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new IfThenElse(config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class IfThenElse extends AbstractCommand {

    private Command conditionChain; 
    private Command thenChain;
    private Command elseChain;
    
    public IfThenElse(Config config, Command parent, Command child, MorphlineContext context) {
      super(config, parent, child, context);
      
      Command devNull = new DropRecordBuilder().build(null, this, null, context); // pipes into /dev/null
      List<Command> conditions = buildCommandChain(config, "conditions", devNull, true);
      if (conditions.size() == 0) {
        throw new MorphlineCompilationException("Missing conditions", config);
      } else {
        this.conditionChain = conditions.get(0);
      }

      List<Command> thenCommands = buildCommandChain(config, "then", child, true);
      if (thenCommands.size() > 0) {
        this.thenChain = thenCommands.get(0);
      }
      
      List<Command> elseCommands = buildCommandChain(config, "else", child, true);
      if (elseCommands.size() > 0) {
        this.elseChain = elseCommands.get(0);
      }
      validateArguments();
    }
    
    protected List<Command> buildCommandChain(Config rootConfig, String configKey, Command finalChild, boolean ignoreNotifications) {
      getConfigs().getConfigList(rootConfig, configKey, null);
      return super.buildCommandChain(rootConfig, configKey, finalChild, ignoreNotifications);
    }
    
    @Override
    protected void doNotify(Record notification) {
      conditionChain.notify(notification);
      if (thenChain != null) {
        thenChain.notify(notification);
      }
      if (elseChain != null) {
        elseChain.notify(notification);
      }
      super.doNotify(notification);
    }
      
    @Override
    protected boolean doProcess(Record record) {
      if (conditionChain.process(record)) {
        if (thenChain != null) {
          return thenChain.process(record);
        }
      } else {
        if (elseChain != null) {
          return elseChain.process(record);
        }
      }
      
      return super.doProcess(record); 
    }  
  }
  
}
