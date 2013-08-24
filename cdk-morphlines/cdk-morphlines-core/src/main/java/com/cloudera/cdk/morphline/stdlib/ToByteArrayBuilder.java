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

import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Collections;
import java.util.ListIterator;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.CommandBuilder;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.AbstractCommand;
import com.google.common.base.Charsets;
import com.typesafe.config.Config;

/**
 * Command that converts the Java objects in a given field via <code>Object.toString()</code> to
 * their string representation, and then via <code>String.getBytes(Charset)</code> to their byte
 * array representation. If the input Java objects are already byte arrays the command does nothing.
 */
public final class ToByteArrayBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("toByteArray");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ToByteArray(config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class ToByteArray extends AbstractCommand {

    private final String fieldName;
    private final Charset charset;
    
    public ToByteArray(Config config, Command parent, Command child, MorphlineContext context) {
      super(config, parent, child, context);      
      this.fieldName = getConfigs().getString(config, "field");
      this.charset = getConfigs().getCharset(config, "charset", Charsets.UTF_8);
      validateArguments();
    }
        
    @Override
    protected boolean doProcess(Record record) {
      ListIterator iter = record.get(fieldName).listIterator();
      while (iter.hasNext()) {
        Object value = iter.next();
        if (!(value instanceof byte[])) {
          String str = value.toString();
          value = str.getBytes(charset);
        }
        iter.set(value);
      }
      return super.doProcess(record);
    }
    
  }
  
}
