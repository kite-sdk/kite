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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
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
 * the stream as a Character Large Object (CLOB).
 */
public final class ReadClobBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("readClob");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ReadClob(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class ReadClob extends AbstractParser {

    private final Charset charset;
    private final String outputFieldName;
    private final char[] buffer = new char[8192];
    private StringBuilder clob; 
    private int counter = 0;
  
    public ReadClob(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);
      this.outputFieldName = getConfigs().getString(config, "outputField", Fields.MESSAGE);
      this.charset = getConfigs().getCharset(config, "charset", null);
      validateArguments();
    }
  
    @Override
    protected boolean doProcess(Record inputRecord, InputStream stream) throws IOException {
      if (counter++ % 8192 == 0) {
        clob = new StringBuilder(); // periodically gc memory from large outlier strings
      }
      incrementNumRecords();
      Charset detectedCharset = detectCharset(inputRecord, charset);  
      Reader reader = new InputStreamReader(stream, detectedCharset);
      clob.setLength(0);
      int len;
      while ((len = reader.read(buffer)) >= 0) {
        clob.append(buffer, 0, len);
      }
      Record outputRecord = inputRecord.copy();
      removeAttachments(outputRecord);
      outputRecord.replaceValues(outputFieldName, clob.toString());
        
      // pass record to next command in chain:
      return getChild().process(outputRecord);
    }
      
  }
}
