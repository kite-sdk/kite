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
package org.kitesdk.morphline.json;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.stdio.AbstractParser;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.typesafe.config.Config;

/**
 * Command that parses an InputStream that contains JSON data; for each JSON object in the stream,
 * the command emits a morphline record containing the object as an attachment in
 * {@link Fields#ATTACHMENT_BODY}.
 */
public final class ReadJsonBuilder implements CommandBuilder {

  /** The MIME type identifier that will be filled into output records */
  public static final String MIME_TYPE = "json/java+memory";

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("readJson");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ReadJson(this, config, parent, child, context);
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class ReadJson extends AbstractParser {

    private final ObjectReader reader;

    public ReadJson(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);
      
      JsonFactory jsonFactory = null;
      String jsonFactoryClassName = getConfigs().getString(config, "jsonFactory", null);
      if (jsonFactoryClassName != null) {
        try {
          jsonFactory = (JsonFactory) Class.forName(jsonFactoryClassName).newInstance();
        } catch (Exception e) {
          throw new MorphlineCompilationException("Cannot create instance", config, e);
        }
      }
      
      String objectMapperClassName = getConfigs().getString(config, "objectMapper", null);
      ObjectMapper objectMapper = null;
      if (objectMapperClassName != null) {
        try {
          objectMapper = (ObjectMapper) Class.forName(objectMapperClassName).newInstance();
        } catch (Exception e) {
          throw new MorphlineCompilationException("Cannot create instance", config, e);
        }
      } else {
        objectMapper = new ObjectMapper(jsonFactory);
      }
      
      String outputClassName = getConfigs().getString(config, "outputClass", JsonNode.class.getName());
      Class outputClass;
      try {
        outputClass = Class.forName(outputClassName);
      } catch (ClassNotFoundException e) {
        throw new MorphlineCompilationException("Class not found", config, e);
      }
      
      reader = objectMapper.reader(outputClass);
      validateArguments();
    }

    @Override
    protected boolean doProcess(Record inputRecord, InputStream in) throws IOException {
      Record template = inputRecord.copy();
      removeAttachments(template);
      MappingIterator iter = reader.readValues(in);
      try {
        while (iter.hasNextValue()) {
          Object rootNode = iter.nextValue();
          incrementNumRecords();
          LOG.trace("jsonObject: {}", rootNode);
          
          Record outputRecord = template.copy();
          outputRecord.put(Fields.ATTACHMENT_BODY, rootNode);
          outputRecord.put(Fields.ATTACHMENT_MIME_TYPE, MIME_TYPE);
  
          // pass record to next command in chain:
          if (!getChild().process(outputRecord)) {
            return false;
          }
        }
        return true;
      } finally {
        iter.close();
      }
    }
  }
}
