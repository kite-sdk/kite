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

import java.util.Collection;
import java.util.Collections;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;

import com.typesafe.config.Config;

/**
 * Factory to create morphline pipe instances.
 */
public final class PipeBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("pipe");
  }
  
  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new Pipe(this, config, (parent != null ? parent : new RootCommand()), child, context);
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  /**
   * The root of the command tree (the parent at the top level).
   */
  private static final class RootCommand implements Command {
    
    @Override
    public Command getParent() {
      return null;
    }
    
    @Override
    public void notify(Record notification) {
      throw new UnsupportedOperationException("Root command should be invisible and must not be called");
    }

    @Override
    public boolean process(Record record) {
      throw new UnsupportedOperationException("Root command should be invisible and must not be called");
    }

  }
}
