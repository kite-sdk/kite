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
package org.kitesdk.morphline.avro;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.shaded.com.google.common.io.Closeables;
import org.kitesdk.morphline.stdio.AbstractParser;

import com.google.common.annotations.Beta;
import com.typesafe.config.Config;


/**
 * Command that converts a morphline record to an Avro record that contains a Map with string keys
 * and array values where the array values can be null, boolean, int, long, float, double, string,
 * bytes.
 */
@Beta
public final class ToAvroMapBuilder implements CommandBuilder {
  
  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("toAvroMap");
  }
  
  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ToAvroMap(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class ToAvroMap extends AbstractCommand {
    
    private final Schema schema;
    
    public ToAvroMap(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);
      
      InputStream in = null;
      try {
        in = getClass().getResourceAsStream("morphlineRecord.avsc");
        this.schema = new Schema.Parser().parse(in);
      } catch (IOException e) {
        throw new MorphlineCompilationException("Cannot parse morphlineRecord schema", config, e, builder);
      } finally {
        Closeables.closeQuietly(in);
      }
      
      validateArguments();
    }
    
    @Override
    protected boolean doProcess(Record inputRecord) {
      Record outputRecord = inputRecord.copy();
      AbstractParser.removeAttachments(outputRecord);
      Map<String, Collection<Object>> map = inputRecord.getFields().asMap();
      map = new HashMap<String, Collection<Object>>(map); // make it mutable
      Field field = schema.getFields().get(0);
      Object avroResult = AvroConversions.toAvro(map, field.schema());
      if (avroResult == AvroConversions.ERROR) {
        LOG.debug("Cannot convert record: {} to schema: {}", inputRecord, schema);
        return false;          
      }
      
      IndexedRecord avroRecord = new GenericData.Record(schema); 
      avroRecord.put(field.pos(), avroResult);
      outputRecord.put(Fields.ATTACHMENT_BODY, avroRecord);
      
      // pass record to next command in chain:
      return super.doProcess(outputRecord);
    }
    
  }
    
}
