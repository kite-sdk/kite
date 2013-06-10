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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.CommandBuilder;
import com.cloudera.cdk.morphline.api.MorphlineCompilationException;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.CSVStrategy;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.reader.CSVReader;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.reader.internal.CSVReaderBuilder;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.reader.internal.DefaultCSVEntryParser;
import com.typesafe.config.Config;

/**
 * Command that extracts zero or more records from the input stream of the first attachment, 
 * representing a Comma Separated Values (CSV) file.
 * 
 * For the format see http://en.wikipedia.org/wiki/Comma-separated_values and
 * http://www.creativyst.com/Doc/Articles/CSV/CSV01.htm and
 * http://ostermiller.org/utils/CSV.html and
 * http://www.ricebridge.com/products/csvman/demo.htm
 * 
 * Note that a quoted field can span multiple lines in the input stream.
 */
public final class ReadCSVBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("readCSV");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ReadCSV(config, parent, child, context);
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
    private final char commentChar = '#';
    private final char quoteChar = '"';
    private final boolean ignoreEmptyLines = true;
  
    public ReadCSV(Config config, Command parent, Command child, MorphlineContext context) {
      super(config, parent, child, context);
      String separator = getConfigs().getString(config, "separator", ",");
      if (separator.length() != 1) {
        throw new MorphlineCompilationException("CSV separator must be one character only: " + separator, config);
      }
      this.separatorChar = separator.charAt(0);
      this.columnNames = getConfigs().getStringList(config, "columns");
      this.charset = getConfigs().getCharset(config, "charset", null);
      this.ignoreFirstLine = getConfigs().getBoolean(config, "ignoreFirstLine", false);
      this.trim = getConfigs().getBoolean(config, "trim", true);      
      validateArguments();
    }
  
    @Override
    protected boolean doProcess(Record inputRecord, InputStream stream) throws IOException {
      Charset detectedCharset = detectCharset(inputRecord, charset);  
      Reader reader = new InputStreamReader(stream, detectedCharset);
      CSVReader<String[]> csvReader = createCSVReader(reader);      
      String[] columnValues;
      
      while ((columnValues = csvReader.readNext()) != null) {
        numRecordsMeter.mark();
        Record outputRecord = inputRecord.copy();
        removeAttachments(outputRecord);
        for (int i = 0; i < columnValues.length; i++) {
          if (i >= columnNames.size()) {
            columnNames.add("column" + i);
          }
          String columnName = columnNames.get(i);
          outputRecord.removeAll(columnName);
          if (columnName.length() > 0) { // empty column name indicates omit this field on output
            outputRecord.put(columnName, trim(columnValues[i]));
          }
        }        
        
        // pass record to next command in chain:
        if (!getChild().process(outputRecord)) {
          return false;
        }
      }
      
      return true;
    }

    private String trim(String str) {
      return trim ? str.trim() : str;
    }
  
    // Uses a shaded version of jcsv-1.4.0 (https://code.google.com/p/jcsv/) 
    // to reduce the potential for dependency conflicts.
    // TODO: consider replacing impl with http://github.com/FasterXML/jackson-dataformat-csv
    // or http://supercsv.sourceforge.net/release_notes.html
    private CSVReader createCSVReader(Reader reader) {
      CSVStrategy strategy = 
          new CSVStrategy(separatorChar, quoteChar, commentChar, ignoreFirstLine, ignoreEmptyLines);
      
      return new CSVReaderBuilder(reader).strategy(strategy).entryParser(new DefaultCSVEntryParser()).build();
    }
  
  }
}
