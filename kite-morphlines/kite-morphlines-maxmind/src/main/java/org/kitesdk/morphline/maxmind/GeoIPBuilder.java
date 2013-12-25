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
package org.kitesdk.morphline.maxmind;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Notifications;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.net.InetAddresses;
import com.maxmind.db.Reader;
import com.typesafe.config.Config;

/**
 * Command that returns Geolocation information for a given IP address, using an efficient in-memory
 * Maxmind database lookup.
 */
public final class GeoIPBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("geoIP");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new GeoIP(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class GeoIP extends AbstractCommand {

    private final String inputFieldName;
    private final File databaseFile;
    private final Reader databaseReader;
    
    
    public GeoIP(CommandBuilder builder, Config config, Command parent, 
                                       Command child, final MorphlineContext context) {
      
      super(builder, config, parent, child, context);      
      this.inputFieldName = getConfigs().getString(config, "inputField");
      this.databaseFile = new File(getConfigs().getString(config, "database", "GeoLite2-City.mmdb"));
      try {
        this.databaseReader = new Reader(databaseFile);
      } catch (IOException e) {
        throw new MorphlineCompilationException("Cannot read Maxmind database: " + databaseFile, config, e);
      }
      validateArguments();
    }

    @Override
    protected boolean doProcess(Record record) {      
      for (Object value : record.get(inputFieldName)) {
        InetAddress addr;
        if (value instanceof InetAddress) {
          addr = (InetAddress) value;
        } else {
          try {
            addr = InetAddresses.forString(value.toString());
          } catch (IllegalArgumentException e) {
            LOG.debug("Invalid IP string literal: {}", value);
            return false;
          }   
        }
        
        JsonNode json;
        try {
          json = databaseReader.get(addr);
        } catch (IOException e) {
          throw new MorphlineRuntimeException("Cannot perform GeoIP lookup for IP: " + addr, e);
        }
        
        ObjectNode location = (ObjectNode) json.get("location");
        if (location != null) {
          JsonNode jlatitude = location.get("latitude");
          JsonNode jlongitude = location.get("longitude");
          if (jlatitude != null && jlongitude != null) {
            String latitude = jlatitude.toString();
            String longitude = jlongitude.toString();
            location.put("latitude_longitude", latitude + "," + longitude);
            location.put("longitude_latitude", longitude + "," + latitude);
          }
        }        
        record.put(Fields.ATTACHMENT_BODY, json);
      }
      
      // pass record to next command in chain:
      return super.doProcess(record);
    }
    
    @Override
    protected void doNotify(Record notification) {      
      for (Object event : Notifications.getLifecycleEvents(notification)) {
        if (event == Notifications.LifecycleEvent.SHUTDOWN) {
          try {
            databaseReader.close();
          } catch (IOException e) {
            LOG.warn("Cannot close Maxmind database: " + databaseFile, e);
          }
        }
      }
      super.doNotify(notification);
    }
    
  }
  
}
