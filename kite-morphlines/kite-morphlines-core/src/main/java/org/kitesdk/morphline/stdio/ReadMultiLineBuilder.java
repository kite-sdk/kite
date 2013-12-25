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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Validator;

import com.typesafe.config.Config;

/**
 * Multiline log parser that collapse multiline messages into a single record; supports "regex",
 * "what" and "negate" configuration parameters similar to logstash.
 * 
 * For example, this can be used to parse log4j with stack traces. Also see
 * https://gist.github.com/smougenot/3182192 and http://logstash.net/docs/1.1.12/filters/multiline
 */
public final class ReadMultiLineBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("readMultiLine");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ReadMultiLine(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class ReadMultiLine extends AbstractParser {

    private final Matcher regex;
    private final boolean negate;
    private final What what;
    private final Charset charset;
  
    public ReadMultiLine(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);
      this.regex = Pattern.compile(getConfigs().getString(config, "regex")).matcher("");
      this.negate = getConfigs().getBoolean(config, "negate", false);
      this.charset = getConfigs().getCharset(config, "charset", null);
      this.what = new Validator<What>().validateEnum(
          config,
          getConfigs().getString(config, "what", What.previous.toString()),
          What.class);
      validateArguments();
    }

    @Override
    protected boolean doProcess(Record inputRecord, InputStream stream) throws IOException {
      Record template = inputRecord.copy();
      removeAttachments(template);
      template.removeAll(Fields.MESSAGE);
      Charset detectedCharset = detectCharset(inputRecord, charset);  
      Reader reader = new InputStreamReader(stream, detectedCharset);
      BufferedReader lineReader = new BufferedReader(reader, getBufferSize(stream));
      StringBuilder lines = null;
      String line;
      
      while ((line = lineReader.readLine()) != null) {
        if (lines == null) {
          lines = new StringBuilder(line);
        } else {
          boolean isMatch = regex.reset(line).matches();
          if (negate) {
            isMatch = !isMatch;
          }
          /*
          not match && previous --> do next
          not match && next     --> do previous
          match && previous     --> do previous
          match && next         --> do next             
          */
          boolean doPrevious = (what == What.previous);
          if (!isMatch) {
            doPrevious = !doPrevious;
          }
          
          if (doPrevious) { // do previous
            lines.append('\n');
            lines.append(line);
          } else {          // do next
            if (lines.length() > 0 && !flushRecord(template.copy(), lines.toString())) {
              return false;
            }
            lines.setLength(0);
            lines.append(line);              
          }
        }          
      }
      if (lines != null && lines.length() > 0) {
        return flushRecord(template.copy(), lines.toString());
      }
      return true;
    }

    private boolean flushRecord(Record outputRecord, String lines) {
      outputRecord.put(Fields.MESSAGE, lines);
      incrementNumRecords();
      
      // pass record to next command in chain:
      return getChild().process(outputRecord);
    }
    
    
    ///////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    ///////////////////////////////////////////////////////////////////////////////
    private static enum What {
      previous,
      next
    }     

  }
}
