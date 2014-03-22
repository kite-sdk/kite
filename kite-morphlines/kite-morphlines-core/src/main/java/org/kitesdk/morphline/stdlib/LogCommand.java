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

import java.util.Collections;
import java.util.List;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.FieldExpression;

import com.typesafe.config.Config;

/**
 * Base class for commands that do slf4j logging; each log level is a subclass.
 */
abstract class LogCommand extends AbstractCommand {
  
  private String format;
  private FieldExpression[] expressions;  
  
  public LogCommand(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
    super(builder, config, parent, child, context);
    this.format = getConfigs().getString(config, "format");
    List<String> argList = getConfigs().getStringList(config, "args", Collections.<String>emptyList());
    this.expressions = new FieldExpression[argList.size()];
    for (int i = 0; i < argList.size(); i++) {
      this.expressions[i] = new FieldExpression(argList.get(i), getConfig()); 
    }
    validateArguments();
  }

  @Override
  protected boolean doProcess(Record record) {
    Object[] resolvedArgs = new Object[expressions.length];
    for (int i = 0; i < expressions.length; i++) {
      resolvedArgs[i] = expressions[i].evaluate(record);
    }
    log(format, resolvedArgs);
    return super.doProcess(record);
  }
  
  protected abstract void log(String format, Object[] args);
     
}
