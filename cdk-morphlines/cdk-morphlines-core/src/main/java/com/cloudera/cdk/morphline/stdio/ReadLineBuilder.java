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
package com.cloudera.cdk.morphline.stdio;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Collections;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.CommandBuilder;
import com.cloudera.cdk.morphline.api.MorphlineCompilationException;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.Fields;
import com.typesafe.config.Config;

/**
 * Command that emits one record per line in the input stream of the first attachment.
 */
public final class ReadLineBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("readLine");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ReadLine(config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class ReadLine extends AbstractParser {

    private final Charset charset;
    private final boolean ignoreFirstLine;
    private final String commentPrefix;
  
    public ReadLine(Config config, Command parent, Command child, MorphlineContext context) {
      super(config, parent, child, context);
      this.charset = getConfigs().getCharset(config, "charset", null);
      this.ignoreFirstLine = getConfigs().getBoolean(config, "ignoreFirstLine", false);
      String cprefix = getConfigs().getString(config, "commentPrefix", "");
      if (cprefix.length() > 1) {
        throw new MorphlineCompilationException("commentPrefix must be at most one character long: " + cprefix, config);
      }
      this.commentPrefix = (cprefix.length() > 0 ? cprefix : null);
      validateArguments();
    }
  
    @Override
    protected boolean doProcess(Record inputRecord, InputStream stream) throws IOException {
      Charset detectedCharset = detectCharset(inputRecord, charset);  
      Reader reader = new InputStreamReader(stream, detectedCharset);
      BufferedReader lineReader = new BufferedReader(reader, getBufferSize(stream));
      boolean isFirst = true;
      String line;

      while ((line = lineReader.readLine()) != null) {
        if (isFirst && ignoreFirstLine) {
          isFirst = false;
          continue; // ignore first line
        }
        if (line.length() == 0) {
          continue; // ignore empty lines
        }
        if (commentPrefix != null && line.startsWith(commentPrefix)) {
          continue; // ignore comments
        }
        Record outputRecord = inputRecord.copy();
        removeAttachments(outputRecord);
        outputRecord.replaceValues(Fields.MESSAGE, line);
        incrementNumRecords();
        
        // pass record to next command in chain:
        if (!getChild().process(outputRecord)) {
          return false;
        }
      }
      return true;        
    }
      
  }
  
}
