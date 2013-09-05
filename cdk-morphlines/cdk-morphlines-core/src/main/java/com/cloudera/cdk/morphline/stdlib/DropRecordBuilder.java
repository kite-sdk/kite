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
package com.cloudera.cdk.morphline.stdlib;

import java.util.Collection;
import java.util.Collections;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.CommandBuilder;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.AbstractCommand;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Command that silently consumes records without ever emitting any record - think /dev/null.
 */
public final class DropRecordBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("dropRecord");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    if (config == null) {
      return new DevNull(parent);
    } else {
      return new DropRecord(config, parent, child, context);
    }
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  /** Implementation that does not do logging and metrics */
  private static final class DevNull implements Command {
    
    private Command parent;
    
    public DevNull(Command parent) { 
      this.parent = parent;
    }

    @Override
    public Command getParent() {
      return parent;
    }
    
    @Override
    public void notify(Record notification) {
    }

    @Override
    public boolean process(Record record) {
      return true;
    }

  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  /** Implementation that does logging and metrics */
  private static final class DropRecord extends AbstractCommand {
    
    public DropRecord(Config config, Command parent, Command child, MorphlineContext context) {
      super(ConfigFactory.empty(), parent, new DummyCommand(), context);
    }

    @Override
    protected void doNotify(Record notification) {
    }

    @Override
    protected boolean doProcess(Record record) {
      return true;
    }

  }


  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  /** Hack because passing child=null to AbstractCommand ctor is illegal */
  private static final class DummyCommand implements Command {
    
    @Override
    public Command getParent() {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public void notify(Record notification) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean process(Record record) {
      throw new UnsupportedOperationException();
    }

  }

}
