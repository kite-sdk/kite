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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import com.codahale.metrics.Meter;
import com.typesafe.config.Config;

/**
 * A tryRules command consists of zero or more rules.
 * 
 * A rule consists of zero or more commands.
 * 
 * The rules of a tryRules command are processed in top-down order. If one of the commands in a rule
 * fails, the tryRules command stops processing of this rule, backtracks and tries the next rule,
 * and so on, until a rule is found that runs all its commands to completion without failure (the
 * rule succeeds). If a rule succeeds the remaining rules of the current tryRules command are
 * skipped. If no rule succeeds the record remains unchanged, but a warning may be issued (the
 * warning can be turned off) or an exception may be thrown (which is logged and ignored in
 * production mode).
 * 
 * Because a command can itself be a tryRules command, there can be tryRules commands with commands,
 * nested inside tryRules, inside tryRules, recursively. This helps to implement arbitrarily complex
 * functionality for advanced usage.
 */
public final class TryRulesBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("tryRules");
  }
  
  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new TryRules(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class TryRules extends AbstractCommand {

    private final List<Command> childRules = new ArrayList<Command>();
    private final boolean throwExceptionIfAllRulesFailed;
    private final boolean catchExceptions;
    private final boolean copyRecords;
    private final Meter numExceptionsCaught;
    
    @SuppressWarnings("unchecked")
    public TryRules(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);
      this.throwExceptionIfAllRulesFailed = getConfigs().getBoolean(config, "throwExceptionIfAllRulesFailed", true);
      this.catchExceptions = getConfigs().getBoolean(config, "catchExceptions", false);
      this.copyRecords = getConfigs().getBoolean(config, "copyRecords", true);
      
      List<? extends Config> ruleConfigs = getConfigs().getConfigList(config, "rules", Collections.EMPTY_LIST);
      for (Config ruleConfig : ruleConfigs) {
        List<Command> commands = buildCommandChain(ruleConfig, "commands", child, true);
        if (commands.size() > 0) {
          childRules.add(commands.get(0));
        }
      }
      validateArguments();
      numExceptionsCaught = getMeter("numExceptionsCaught");
    }
    
    @Override
    protected void doNotify(Record notification) {
      for (Command childRule : childRules) {
        if (!catchExceptions) {
          childRule.notify(notification);
        } else {
          try {
            childRule.notify(notification);
          } catch (RuntimeException e) {
            numExceptionsCaught.mark();
            LOG.warn("tryRules command caught rule exception in doNotify(). Continuing to try other remaining rules", e);
            // continue and try the other remaining rules
          }
        }
      }
      super.doNotify(notification);
    }
  
    @Override
    protected boolean doProcess(Record record) {
      for (Command childRule : childRules) {
        Record copy = copyRecords ? record.copy() : record;
        if (!catchExceptions) {
          if (childRule.process(copy)) {
            return true; // rule was executed successfully; no need to try the other remaining rules
          }          
        } else {
          try {
            if (childRule.process(copy)) {
              return true; // rule was executed successfully; no need to try the other remaining rules
            }
          } catch (RuntimeException e) {
            numExceptionsCaught.mark();
            LOG.warn("tryRules command caught rule exception in doProcess(). Continuing to try other remaining rules", e);
            // continue and try the other remaining rules
          }
        }
      }
      LOG.warn("tryRules command found no successful rule");
      if (throwExceptionIfAllRulesFailed) {
        throw new MorphlineRuntimeException("tryRules command found no successful rule for record: " + record);
      }
      return false;
    }
    
  }
  
}
