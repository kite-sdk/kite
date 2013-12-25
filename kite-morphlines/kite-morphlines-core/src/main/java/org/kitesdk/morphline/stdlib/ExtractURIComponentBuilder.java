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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Validator;

import com.typesafe.config.Config;

/**
 * A command that extracts a subcomponent from the URIs contained in the given input field and
 * appends it to the given output fields.
 */
public final class ExtractURIComponentBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("extractURIComponent");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ExtractURIComponent(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class ExtractURIComponent extends AbstractCommand {

    private final String inputFieldName;
    private final String outputFieldName;
    private final boolean failOnInvalidURI;
    private final Component component;
    
    public ExtractURIComponent(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);      
      this.inputFieldName = getConfigs().getString(config, "inputField");
      this.outputFieldName = getConfigs().getString(config, "outputField");
      this.failOnInvalidURI = getConfigs().getBoolean(config, "failOnInvalidURI", false);
      this.component = new Validator<Component>().validateEnum(
          config,
          getConfigs().getString(config, "component"),
          Component.class);
      validateArguments();
    }
        
    @Override
    protected boolean doProcess(Record record) {
      for (Object uriObj : record.get(inputFieldName)) {
        URI uri;
        try {
          uri = new URI(uriObj.toString());
        } catch (URISyntaxException e) {
          LOG.debug("Invalid URI: {}", uriObj);
          if (failOnInvalidURI) {
            return false;
          }
          continue;
        }
        
        switch (component) {
          case scheme: {
            addValue(record, uri.getScheme());
            break;
          }
          case authority: {
            addValue(record, uri.getAuthority());
            break;
          }
          case path: {
            addValue(record, uri.getPath());
            break;
          }
          case query: {
            addValue(record, uri.getQuery());
            break;
          }
          case fragment: {
            addValue(record, uri.getFragment());
            break;
          }
          case host: {
            addValue(record, uri.getHost());
            break;
          }
          case port: {
            addValue(record, uri.getPort());
            break;
          }
          case schemeSpecificPart: {
            addValue(record, uri.getSchemeSpecificPart());
            break;
          }
          case userInfo: {
            addValue(record, uri.getUserInfo());
            break;
          }
          default: {
            throw new IllegalStateException("Illegal component: " + component.name()); // unreachable
          }
        }
      }
      
      // pass record to next command in chain:
      return super.doProcess(record);
    }
    
    private void addValue(Record record, Object value) {
      if (value != null) {
        record.put(outputFieldName, value);
      }
    }
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static enum Component {
    scheme,
    authority,
    path,
    query,
    fragment,
    host,
    port,
    schemeSpecificPart,
    userInfo,
  }     

}
