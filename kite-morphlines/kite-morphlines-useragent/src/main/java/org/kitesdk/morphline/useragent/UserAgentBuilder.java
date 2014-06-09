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
package org.kitesdk.morphline.useragent;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Configs;
import org.kitesdk.morphline.base.Metrics;
import org.kitesdk.morphline.base.Validator;

import ua_parser.Client;
import ua_parser.Parser;

import com.codahale.metrics.Meter;
import com.google.common.base.Preconditions;
import org.kitesdk.morphline.shaded.com.google.common.io.Closeables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Command that parses user agent strings and returns structured higher level data like user agent
 * family, operating system, version, and device type, using the underlying API and regexes.yaml
 * BrowserScope database from https://github.com/tobie/ua-parser.
 */
public final class UserAgentBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("userAgent");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new UserAgent(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class UserAgent extends AbstractCommand {

    private final String inputFieldName;
    private final List<Mapping> mappings = new ArrayList();
    
    public UserAgent(CommandBuilder builder, Config config, Command parent, 
                     Command child, MorphlineContext context) {
      
      super(builder, config, parent, child, context);      
      this.inputFieldName = getConfigs().getString(config, "inputField");
      String databaseFile = getConfigs().getString(config, "database", null);
      int cacheCapacity = getConfigs().getInt(config, "cacheCapacity", 1000);
      String nullReplacement = getConfigs().getString(config, "nullReplacement", "");

      Parser parser;
      try {
        if (databaseFile == null) {
          parser = new Parser(); 
        } else {
          InputStream in = new BufferedInputStream(new FileInputStream(databaseFile));
          try {
            parser = new Parser(in);
          } finally {
            Closeables.closeQuietly(in);
          }
        }        
      } catch (IOException e) {
        throw new MorphlineCompilationException("Cannot parse UserAgent database: " + databaseFile, config, e);
      }
      
      Meter numCacheHitsMeter = isMeasuringMetrics() ? getMeter(Metrics.NUM_CACHE_HITS) : null;
      Meter numCacheMissesMeter = isMeasuringMetrics() ? getMeter(Metrics.NUM_CACHE_MISSES) : null;
      
      Config outputFields = getConfigs().getConfig(config, "outputFields", ConfigFactory.empty());
      for (Map.Entry<String, Object> entry : new Configs().getEntrySet(outputFields)) {
        mappings.add(
            new Mapping(
                entry.getKey(), 
                entry.getValue().toString().trim(), 
                parser, 
                new BoundedLRUHashMap(cacheCapacity), 
                nullReplacement, 
                config,
                numCacheHitsMeter,
                numCacheMissesMeter
                ));
      }
      validateArguments();
    }

    @Override
    protected boolean doProcess(Record record) {      
      for (Object value : record.get(inputFieldName)) {
        Preconditions.checkNotNull(value);
        String stringValue = value.toString().trim();
        for (Mapping mapping : mappings) {
          mapping.apply(record, stringValue);
        }
      }
      
      // pass record to next command in chain:
      return super.doProcess(record);
    }

  }


  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class Mapping {
    
    private final String fieldName;
    private final List components = new ArrayList();
    private final Parser parser;    
    private final Map<String, String> cache;
    private final String nullReplacement;
    
    private final Meter numCacheHitsMeter;
    private final Meter numCacheMissesMeter;
    
    private static final String START_TOKEN = "@{";
    private static final char END_TOKEN = '}';
    
    public Mapping(String fieldName, String expression, Parser parser, Map<String, String> cache,
        String nullReplacement, Config config, Meter numCacheHitsMeter, Meter numCacheMissesMeter) {
      
      this.fieldName = fieldName;
      this.parser = parser;
      this.cache = cache;
      Preconditions.checkNotNull(nullReplacement);
      this.nullReplacement = nullReplacement;
      this.numCacheHitsMeter = numCacheHitsMeter;
      this.numCacheMissesMeter = numCacheMissesMeter;
      int from = 0;
      
      while (from < expression.length()) {
        int start = expression.indexOf(START_TOKEN, from);
        if (start < 0) { // START_TOKEN not found
          components.add(expression.substring(from, expression.length()));
          return;
        } else { // START_TOKEN found
          if (start > from) {
            components.add(expression.substring(from, start));
          }
          int end = expression.indexOf(END_TOKEN, start + START_TOKEN.length());
          if (end < 0) {
            throw new IllegalArgumentException("Missing closing token: " + END_TOKEN);
          }
          String ref = expression.substring(start + START_TOKEN.length(), end);
          components.add(new Validator<Component>().validateEnum(
              config,
              ref,
              Component.class));
          from = end + 1;
        }
      }
    }
    
    public void apply(Record record, String userAgent) {
      String result = cache.get(userAgent);
      if (result == null) { // cache miss
        if (numCacheMissesMeter != null) {
          numCacheMissesMeter.mark();
        }
        result = extract(userAgent);
        cache.put(userAgent, result);
      } else if (numCacheHitsMeter != null) {
        numCacheHitsMeter.mark();
      }
      record.put(fieldName, result);
    }

    private String extract(String userAgent) {
      Client client = parser.parse(userAgent);        
      StringBuilder buf = new StringBuilder();
      String lastString = null;
      
      for (Object component : components)  {
        assert component != null;
        if (component instanceof Component) {
          String result = resolve((Component)component, client);
          if (result == null) {
            result = nullReplacement;
          }
          
          // suppress preceding string separator if component resolves to empty string:
          if (result.length() > 0 && lastString != null) {
            buf.append(lastString); 
          }
          
          buf.append(result);
          lastString = null;
        } else {
          lastString = (String)component;
        }
      }
      
      if (lastString != null) {
        buf.append(lastString);
      }
      return buf.toString();
    }

    private String resolve(Component component, Client client) {
      switch (component) {
        case ua_family : {
          return client.userAgent.family;
        }
        case ua_major : {
          return client.userAgent.major;
        }
        case ua_minor : {
          return client.userAgent.minor;
        }
        case ua_patch : {
          return client.userAgent.patch;
        }
        case os_family : {
          return client.os.family;
        }
        case os_major : {
          return client.os.major;
        }
        case os_minor : {
          return client.os.minor;
        }
        case os_patch : {
          return client.os.patch;
        }
        case os_patch_minor : {
          return client.os.patchMinor;
        }
        case device_family : {
          return client.device.family;
        }
        default : {
          throw new IllegalArgumentException();
        }
      }
    }
  }
  

  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static enum Component {
    ua_family,
    ua_major,
    ua_minor,
    ua_patch,
    os_family,
    os_major,
    os_minor,
    os_patch,
    os_patch_minor,
    device_family
  }     

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class BoundedLRUHashMap<K,V> extends LinkedHashMap<K,V> {
    
    private final int capacity;

    private BoundedLRUHashMap(int capacity) {
      super(16, 0.5f, true);
      this.capacity = capacity;
    }
    
    @Override
    protected boolean removeEldestEntry(Map.Entry eldest) {
      return size() > capacity;
    }
      
  } 

}
