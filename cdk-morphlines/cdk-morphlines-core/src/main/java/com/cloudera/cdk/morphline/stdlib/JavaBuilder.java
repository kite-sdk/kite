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

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;

import javax.script.ScriptException;

import org.slf4j.Logger;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.CommandBuilder;
import com.cloudera.cdk.morphline.api.MorphlineCompilationException;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.MorphlineRuntimeException;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.AbstractCommand;
import com.cloudera.cdk.morphline.base.Configs;
import com.cloudera.cdk.morphline.base.Fields;
import com.cloudera.cdk.morphline.scriptengine.java.ScriptEvaluator;
import com.typesafe.config.Config;

/**
 * The "java" command compiles the given Java code block, wrapped into a Java method with a boolean
 * return type and several parameters, along with a Java class definition that contains the given
 * import statements.
 * <p>
 * The parameters passed to the code block are "record", "config", "parent", "child", "context",
 * "logger" and are of types Record.class, Config.class, Command.class, Command.class,
 * MorphlineContext.class, Logger.class, respectively.
 * <p>
 * Compilation is done in main memory, i.e. without writing to the filesystem.
 * <p>
 * The result is an object that can be executed (and reused) any number of times. This is a high
 * performance implementation, using an optimized variant of https://scripting.dev.java.net/" (JSR
 * 223 Java Scripting). Calling {@link #evaluate(Object...)} just means calling
 * {@link Method#invoke(Object, Object...)} and as such has the same minimal runtime cost, i.e.
 * O(100M calls/sec/core).
 */
public final class JavaBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("java");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    try {
      return new Java(config, parent, child, context);
    } catch (ScriptException e) {
      throw new MorphlineCompilationException("Cannot compile script", config, e);
    }
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class Java extends AbstractCommand {

    private final ScriptEvaluator<Boolean> script;
    
    private static final String DEFAULT_IMPORTS = 
        "import java.util.*;\n" +
        "import java.io.*;\n" +
        "import java.text.*;\n" +
        "import com.google.common.base.*;\n" + 
        "import com.google.common.collect.*;\n" +
        "import " + Record.class.getPackage().getName() + ".*;\n" +
        "import " + Fields.class.getName() + ";\n" +
        "import " + Configs.class.getName() + ";\n" +
        "";
    
    public Java(Config config, Command parent, Command child, MorphlineContext context) throws ScriptException {
      super(config, parent, child, context);
      
      String javaImports = getConfigs().getString(config, "imports", DEFAULT_IMPORTS);
      String javaCodeBlock = getConfigs().getString(config, "code");
      this.script = new ScriptEvaluator<Boolean>(
          javaImports, 
          javaCodeBlock, 
          Boolean.class,
          new String[] {"record", "config", "parent", "child", "context", "logger"}, 
          new Class[] {Record.class, Config.class, Command.class, Command.class, MorphlineContext.class, Logger.class}, 
          javaCodeBlock
          );
      validateArguments();
    }
        
    @Override
    protected boolean doProcess(Record record) { 
      try {
        return script.evaluate(record, getConfig(), this, getChild(), getContext(), LOG);
      } catch (ScriptException e) {
        throw new MorphlineRuntimeException("Cannot execute script", e);
      } 
    }  
  }
  
}
