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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import com.typesafe.config.Config;

/**
 * A command that adds the name or IP of the local host to a given output field.
 */
public final class AddLocalHostBuilder implements CommandBuilder {

  public static final String FIELD_NAME = "field";
  public static final String PRESERVE_EXISTING_NAME = "preserveExisting";
  public static final String USE_IP = "useIP";  
  
  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("addLocalHost");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new AddLocalHost(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class AddLocalHost extends AbstractCommand {
    
    private final String fieldName;
    private final boolean preserveExisting;
    private final String host;

    public AddLocalHost(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) { 
      super(builder, config, parent, child, context);
      this.fieldName = getConfigs().getString(config, FIELD_NAME, "host");
      this.preserveExisting = getConfigs().getBoolean(config, PRESERVE_EXISTING_NAME, true);
      boolean useIP = getConfigs().getBoolean(config, USE_IP, true);      
      validateArguments();
      
      InetAddress addr = null;
      try {
        addr = InetAddress.getLocalHost();
      } catch (UnknownHostException e) {
        LOG.warn("Cannot get address of local host", e);
      }
      
      if (addr == null) {
        host = null;
      } else if (useIP) {
        host = addr.getHostAddress();
      } else {
        host = addr.getCanonicalHostName();
      }
    }

    @Override
    protected boolean doProcess(Record record) {      
      if (preserveExisting && record.getFields().containsKey(fieldName)) {
        ; // we must preserve the existing host
      } else {
        record.removeAll(fieldName);
        if (host != null) {
          record.put(fieldName, host);
        }
      }
      
      // pass record to next command in chain:
      return super.doProcess(record);
    }

  }

}
