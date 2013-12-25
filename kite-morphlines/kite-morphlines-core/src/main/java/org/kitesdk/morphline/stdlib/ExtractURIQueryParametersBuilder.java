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

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Collections;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import com.typesafe.config.Config;

/**
 * A command that extracts the query parameters with a given name from the URIs contained in the
 * given input field and appends them to the given output field.
 */
public final class ExtractURIQueryParametersBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("extractURIQueryParameters");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ExtractURIQueryParameters(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class ExtractURIQueryParameters extends AbstractCommand {

    private final String parameterName;
    private final String inputFieldName;
    private final String outputFieldName;
    private final int maxParameters;
    private final String charset;
    private final boolean failOnInvalidURI;
    
    public ExtractURIQueryParameters(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);      
      this.parameterName = getConfigs().getString(config, "parameter");
      this.inputFieldName = getConfigs().getString(config, "inputField");
      this.outputFieldName = getConfigs().getString(config, "outputField");
      this.failOnInvalidURI = getConfigs().getBoolean(config, "failOnInvalidURI", false);
      this.maxParameters = getConfigs().getInt(config, "maxParameters", Integer.MAX_VALUE);
      this.charset = getConfigs().getString(config, "charset", "UTF-8");
      Charset.forName(charset); // fail fast if charset is unsupported
      validateArguments();
    }
        
    @Override
    protected boolean doProcess(Record record) {
      for (Object uri : record.get(inputFieldName)) {
        String rawURIQuery = "";
        try {
          rawURIQuery = new URI(uri.toString()).getRawQuery();
        } catch (URISyntaxException e) {
          LOG.debug("Invalid URI: {}", uri);
          if (failOnInvalidURI) {
            return false;
          }
        }
        if (rawURIQuery == null) {
          rawURIQuery = "";
        }
        
        int len = rawURIQuery.length();
        int i = 0;
        int numParameters = 0;
        while (numParameters < maxParameters && i < len) {
          int j = rawURIQuery.indexOf('&', i);
          if (j < 0) {
            j = len;
          }
          for (int k = i; k < j; k++) {
            if (rawURIQuery.charAt(k) == '=') {
              String key = decodeString(rawURIQuery.substring(i, k));
              if (parameterName.equals(key)) {
                String value = decodeString(rawURIQuery.substring(k + 1, j));
                record.put(outputFieldName, value);
                numParameters++;
              }              
              break; // found '=' separator
            }
          }
          i = j;
          i++;
        }
      }
      
      // pass record to next command in chain:
      return super.doProcess(record);
    }

    private String decodeString(String str) {
      // use commons codec for perf? return new URLCodec().decode(str, charset);
      try {
        return URLDecoder.decode(str, charset);
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e); // unreachable
      }
    }

  }
  
}
