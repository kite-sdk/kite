/**
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
package org.kitesdk.morphline.shaded.com.googlecode.jcsv.fastreader;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;

import org.kitesdk.morphline.api.Record;


/**
 * A very simple csv tokenizer implementation.
 * If you do not need field quotations or multi line columns, this
 * will serve your purposes.
 *
 */
public final class SimpleCSVTokenizer implements CSVTokenizer {
  
  private final char separatorChar;
  private final boolean trim;
  private final List<String> columnNames;
  private final boolean addEmptyStrings; 
  
  public SimpleCSVTokenizer(char separatorChar, boolean trim, boolean addEmptyStrings, List<String> columnNames) {
    this.separatorChar = separatorChar;
    this.trim = trim;
    this.addEmptyStrings = addEmptyStrings;
    this.columnNames = columnNames;    
  }
  
  /** Splits the given input line into parts, using the given delimiter. */
  @Override
  public boolean tokenizeLine(String line, BufferedReader reader, Record record) throws IOException {
    char separator = separatorChar;
    int len = line.length();
    int start = 0; 
    int j = 0;
    for (int i = 0; i < len; i++) {
      if (line.charAt(i) == separator) {
        put(line, start, i, j, record);
        start = i+1;
        j++;
      }
    }
    put(line, start, len, j, record);
    return true;
  }

  private void put(String line, int start, int i, int j, Record record) {
    if (j >= columnNames.size()) {
      columnNames.add("column" + j);
    }
    String columnName = columnNames.get(j);
    if (columnName.length() != 0) { // empty column name indicates omit this field on output
      String col = line.substring(start, i);
      col = trim ? col.trim() : col;
      if (col.length() > 0 || addEmptyStrings) {
        record.put(columnName, col);
      }
    }
  }
}
