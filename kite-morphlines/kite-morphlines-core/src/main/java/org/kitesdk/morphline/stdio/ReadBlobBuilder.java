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
package org.kitesdk.morphline.stdio;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;

import com.typesafe.config.Config;

/**
 * Command that emits one record for the entire input stream of the first attachment, interpreting
 * the stream as a Binary Large Object (BLOB), i.e. emits a corresponding Java byte array.
 */
public final class ReadBlobBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("readBlob");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ReadBlob(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class ReadBlob extends AbstractParser {

    private final String outputFieldName;
    private final byte[] buffer = new byte[8192];
    private ByteArrayOutputStream blob = null;
    private int counter = 0;
  
    public ReadBlob(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);
      this.outputFieldName = getConfigs().getString(config, "outputField", Fields.ATTACHMENT_BODY);
      validateArguments();
    }
  
    @Override
    protected boolean doProcess(Record inputRecord, InputStream stream) throws IOException {
      if (counter++ % 8192 == 0) {
        blob = new ByteArrayOutputStream(8192); // periodically gc memory from large outlier BLOBs
      }
      incrementNumRecords();
      
      blob.reset();      
      int len;
      while ((len = stream.read(buffer)) >= 0) {
        blob.write(buffer, 0, len);
      }      
      inputRecord.replaceValues(outputFieldName, blob.toByteArray());
        
      // pass record to next command in chain:
      return getChild().process(inputRecord);
    }
      
  }
  
}
