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
package com.cloudera.cdk.morphline.json;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.CommandBuilder;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.Fields;
import com.cloudera.cdk.morphline.stdio.AbstractParser;
import com.fasterxml.jackson.databind.JsonNode;
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
    return new ReadJson(config, parent, child, context);
  }

  
  // /////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  // /////////////////////////////////////////////////////////////////////////////
  private static final class ReadJson extends AbstractParser {

    private final ObjectReader reader = new ObjectMapper().reader(JsonNode.class);

    public ReadJson(Config config, Command parent, Command child, MorphlineContext context) {
      super(config, parent, child, context);
      validateArguments();
    }

    @Override
    protected boolean doProcess(Record inputRecord, InputStream in) throws IOException {
      Iterator<JsonNode> iter = reader.readValues(in);
      while (iter.hasNext()) {
        JsonNode rootNode = iter.next();
        incrementNumRecords();
        LOG.debug("jsonObject: {}", rootNode);
        
        Record outputRecord = inputRecord.copy();
        removeAttachments(outputRecord);
        outputRecord.put(Fields.ATTACHMENT_BODY, rootNode);
        outputRecord.put(Fields.ATTACHMENT_MIME_TYPE, MIME_TYPE);

        // pass record to next command in chain:
        if (!getChild().process(outputRecord)) {
          return false;
        }
      }
      return true;
    }
  }
}
