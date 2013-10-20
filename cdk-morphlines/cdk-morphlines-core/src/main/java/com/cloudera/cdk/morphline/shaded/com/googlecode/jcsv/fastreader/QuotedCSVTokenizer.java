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
package com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.fastreader;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;

import com.cloudera.cdk.morphline.api.Record;


/**
 * This implementation follows the csv formatting standard, described in:
 * http://en.wikipedia.org/wiki/Comma-separated_values
 *
 * If you have a more specific csv format, such as constant column widths or
 * columns that do not need to be quoted, you may consider to write a more simple
 * but performant CSVTokenizer.
 *
 */
public final class QuotedCSVTokenizer implements CSVTokenizer {
  
  private final char separatorChar;
  private final boolean trim;
  private final List<String> columnNames;
  private final char quoteChar;
  
  public QuotedCSVTokenizer(char separatorChar, boolean trim, List<String> columnNames, char quoteChar) {
    this.separatorChar = separatorChar;
    this.trim = trim;
    this.columnNames = columnNames;
    this.quoteChar = quoteChar;
  }
  
  /** Splits the given input line into parts, using the given delimiter. */
  @Override
  public void tokenizeLine(String line, BufferedReader reader, Record record) throws IOException {
    final char DELIMITER = separatorChar;
    final char QUOTE = quoteChar;
    final char NEW_LINE = '\n';

    final StringBuilder sb = new StringBuilder(30);
    boolean isQuoted = false;
    int i = 0;
    int j = 0;
    while (i < line.length() || isQuoted) {
      
      if (!isQuoted) {
        final char c = line.charAt(i);
        if (c == DELIMITER) {
          put(sb, j, record);
          j++;
          sb.setLength(0);
        } else if (c == QUOTE) {
          if (sb.length() == 0) {
            isQuoted = true;
          } else if (i + 1 < line.length() && line.charAt(i + 1) == QUOTE) {
            sb.append(c);
            i++;
          } else {
            isQuoted = true;
          }
        } else {
          sb.append(c);
        }
          
      } else {
        assert isQuoted;
        if (i == line.length()) {
          sb.append(NEW_LINE);
          i = -1;
          line = reader.readLine();
          if (line == null) {
            throw new IllegalStateException("unexpected end of file, unclosed quotation");
          }
        } else {
          final char c = line.charAt(i);
          if (c == QUOTE) {
            if (i + 1 < line.length() && line.charAt(i + 1) == QUOTE) {
              sb.append(c); // found two quotes -> insert single quote
              i++;
            } else {
              isQuoted = false;
            }
          } else {
            sb.append(c);
          }
        }
      }

      i++;
    }
    
    if (!(j == 0 && sb.length() == 0)) {
      put(sb, j, record);
    }
  }
  
  private void put(StringBuilder sb, int j, Record record) {
    if (j >= columnNames.size()) {
      columnNames.add("column" + j);
    }
    String columnName = columnNames.get(j);
    if (columnName.length() != 0) { // empty column name indicates omit this field on output
      String col = sb.toString();
      col = trim ? col.trim() : col;
      record.put(columnName, col);
    }
  }

}
