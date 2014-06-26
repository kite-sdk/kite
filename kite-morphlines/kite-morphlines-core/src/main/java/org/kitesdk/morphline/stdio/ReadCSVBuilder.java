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
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Validator;
import org.kitesdk.morphline.shaded.com.googlecode.jcsv.fastreader.CSVTokenizer;
import org.kitesdk.morphline.shaded.com.googlecode.jcsv.fastreader.QuotedCSVTokenizer;
import org.kitesdk.morphline.shaded.com.googlecode.jcsv.fastreader.SimpleCSVTokenizer;

import com.typesafe.config.Config;

/**
 * Command that extracts zero or more records from the input stream of the first attachment, 
 * representing a Comma Separated Values (CSV) file.
 * 
 * For the format see http://www.creativyst.com/Doc/Articles/CSV/CSV01.htm.
 */
public final class ReadCSVBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("readCSV");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ReadCSV(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class ReadCSV extends AbstractParser {

    private final char separatorChar;
    private final List<String> columnNames;
    private final Charset charset;
    private final boolean ignoreFirstLine;
    private final boolean trim;
    private final boolean addEmptyStrings;
    private final String commentPrefix;
    private final String quoteChar;
    private final boolean ignoreEmptyLines = true;
    private final int maxCharactersPerRecord;
    private final boolean ignoreTooLongRecords;
    private final CSVTokenizer tokenizer;
  
    public ReadCSV(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);
      String separator = getConfigs().getString(config, "separator", ",");
      if (separator.length() != 1) {
        throw new MorphlineCompilationException("CSV separator must be one character only: " + separator, config);
      }
      this.separatorChar = separator.charAt(0);
      this.columnNames = getConfigs().getStringList(config, "columns");
      this.charset = getConfigs().getCharset(config, "charset", null);
      this.ignoreFirstLine = getConfigs().getBoolean(config, "ignoreFirstLine", false);
      this.trim = getConfigs().getBoolean(config, "trim", true);      
      this.addEmptyStrings = getConfigs().getBoolean(config, "addEmptyStrings", true);
      this.quoteChar = getConfigs().getString(config, "quoteChar", "");
      if (quoteChar.length() > 1) {
        throw new MorphlineCompilationException(
            "Quote character must not have a length of more than one character: " + quoteChar, config);
      }
      if (quoteChar.equals(String.valueOf(separatorChar))) {
        throw new MorphlineCompilationException(
            "Quote character must not be the same as separator: " + quoteChar, config);
      }
      this.commentPrefix = getConfigs().getString(config, "commentPrefix", "");
      if (commentPrefix.length() > 1) {
        throw new MorphlineCompilationException(
            "Comment prefix must not have a length of more than one character: " + commentPrefix, config);
      }
      this.maxCharactersPerRecord = getConfigs().getInt(config, "maxCharactersPerRecord", 1000 * 1000);
      this.ignoreTooLongRecords = new Validator<OnMaxCharactersPerRecord>().validateEnum(
          config,
          getConfigs().getString(config, "onMaxCharactersPerRecord", OnMaxCharactersPerRecord.throwException.toString()),
          OnMaxCharactersPerRecord.class) == OnMaxCharactersPerRecord.ignoreRecord;
      this.tokenizer = quoteChar.length() == 0 ? 
          new SimpleCSVTokenizer(separatorChar, trim, addEmptyStrings, columnNames) : 
          new QuotedCSVTokenizer(separatorChar, trim, addEmptyStrings, columnNames, maxCharactersPerRecord, ignoreTooLongRecords, quoteChar.charAt(0));          
      validateArguments();
    }
  
    @Override
    protected boolean doProcess(Record inputRecord, InputStream stream) throws IOException {
      Record template = inputRecord.copy();
      removeAttachments(template);
      Charset detectedCharset = detectCharset(inputRecord, charset);  
      BufferedReader reader = new BufferedReader(
          new InputStreamReader(stream, detectedCharset), getBufferSize(stream));
      if (ignoreFirstLine) {
        reader.readLine();
      }      

      while (true) {
        Record outputRecord = readNext(reader, template);
        if (outputRecord == null) {
          break;
        }
        incrementNumRecords();
        
        // pass record to next command in chain:
        if (!getChild().process(outputRecord)) {
          return false;
        }
      }
      return true;
    }

    private Record readNext(BufferedReader reader, Record template) throws IOException {
      while (true) {
        String line = reader.readLine();
        if (line == null) {
          return null;
        }

        if (!QuotedCSVTokenizer.verifyRecordLength(
            line.length(), maxCharactersPerRecord, line, ignoreTooLongRecords, LOG)) {
          continue; // ignore
        }
        
        if (ignoreEmptyLines && isTrimmedLineEmpty(line)) {
          continue; // ignore
        }

        if (commentPrefix.length() > 0 && line.startsWith(commentPrefix)) {
          continue; // ignore
        }

        Record outputRecord = template.copy();
        if (!tokenizer.tokenizeLine(line, reader, outputRecord)) {
          continue; // ignore
        }
        
        return outputRecord;
      }
    }
    
    private boolean isTrimmedLineEmpty(String line) {
//      return line.trim().length() == 0; // slow
      int len = line.length();
      for (int i = len; --i >= 0; ) {
        if (line.charAt(i) > ' ') {
          return false;
        }
      }
      return true;
    }

    
    ///////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    ///////////////////////////////////////////////////////////////////////////////
    private static enum OnMaxCharactersPerRecord {
      ignoreRecord,
      throwException,
    }     
    
  }
}
